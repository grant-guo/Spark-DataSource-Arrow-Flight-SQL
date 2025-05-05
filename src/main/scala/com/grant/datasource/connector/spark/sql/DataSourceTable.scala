package com.grant.datasource.connector.spark.sql

import com.grant.datasource.connector.spark.sql.conf.DatastoreConfig
import com.grant.datasource.connector.spark.sql.res.ResourceAllocator.allocator
import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.flight.{FlightClient, FlightInfo, Location}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Schema}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.JavaConverters
import scala.jdk.CollectionConverters
import scala.util.{Failure, Success, Try}

/**
 * when spark creates the table, contact Arrow Flight server by running the SQL query in order to retrieve the schema
 * of the query
 * @param config
 */
class DataSourceTable(val config: DatastoreConfig) extends Table with SupportsRead{
  private val logger: Logger = LoggerFactory.getLogger(classOf[DataSourceTable])

  private lazy val flightInfo: FlightInfo = {
    config.arrowFlightUrls.foldLeft(Option[FlightInfo](null))((feClientOpt, url) => {
      feClientOpt match {
        case Some(_) => feClientOpt
        case None => {
          // failed to retrieve flight endpoints from the previous fe url, will try the current one

          Try({
            val urlRegex = "(.+):([0-9]+)".r
            url match {
              case urlRegex(host, port) => {
                val feLocation = Location.forGrpcInsecure(host, port.toInt)
                val feClient = FlightClient.builder(allocator, feLocation).build()
                val credentialCallOption = feClient.authenticateBasicToken(config.user, config.password).get()
                val feSqlClient = new FlightSqlClient(feClient)
                val sql = config.tableOrQuery match {
                  case Left(table) => s"SELECT * FROM ${config.database}.${table}"
                  case Right(query) => query
                }
                val preparedStatement = feSqlClient.prepare(sql, credentialCallOption)
                val flightInfo = preparedStatement.execute(credentialCallOption)
                feSqlClient.close()
                Some(flightInfo)
              }
              case _ => {
                logger.info("invalid fe.http.url: {}", url)
                Option[FlightInfo](null)
              }
            }
          }) match {
            case Failure(exception) => {
              logger.info("Failed to retrieve the flight endpoints from the url: {}, will try the next", url)
              Option[FlightInfo](null)
            }
            case Success(value) => value
          }
        } //end of feClientOpt match - case None
      } //end of feClientOpt match
    }) match {
      case None => throw new IllegalArgumentException("failed to retrieve the flight endpoints from the fe.arrow.flight.url")
      case Some(flightInfo) => flightInfo
    }
  }

  override def name(): String = "datastore-arrow-flight-sql"

  override lazy val schema: StructType = {

    val originalSchema: Schema = this.flightInfo.getSchemaOptional.orElseThrow(() => new IllegalArgumentException("schema is not available"))
    val fields = CollectionConverters.ListHasAsScala(originalSchema.getFields).asScala.map(field => {
      val fieldType = field.getFieldType
      val nullable = fieldType.isNullable
      val name = field.getName
      fieldType.getType match {
        case i: ArrowType.Int if i.getBitWidth <= 8 && i.getIsSigned => StructField(name, ByteType, nullable = nullable)
        case i: ArrowType.Int if i.getBitWidth <= 8 && !i.getIsSigned => StructField(name, ShortType, nullable = nullable)
        case i: ArrowType.Int if i.getBitWidth > 8 && i.getBitWidth <= 16 && i.getIsSigned => StructField(name, ShortType, nullable = nullable)
        case i: ArrowType.Int if i.getBitWidth > 8 && i.getBitWidth <= 16 && !i.getIsSigned => StructField(name, IntegerType, nullable = nullable)
        case i: ArrowType.Int if i.getBitWidth > 16 && i.getBitWidth <= 32 && i.getIsSigned => StructField(name, IntegerType, nullable = nullable)
        case i: ArrowType.Int if i.getBitWidth > 16 && i.getBitWidth <= 32 && !i.getIsSigned => StructField(name, LongType, nullable = nullable)
        case i: ArrowType.Int if i.getBitWidth > 32 && i.getBitWidth <= 64 && i.getIsSigned => StructField(name, LongType, nullable = nullable)
        case i: ArrowType.Int if i.getBitWidth > 64 && !i.getIsSigned => StructField(name, StringType, nullable = nullable) // LARGEINT in StarRocks
        case i: ArrowType.Bool => StructField(name, BooleanType, nullable = nullable)
        case i: ArrowType.FloatingPoint if i.getPrecision.equals(FloatingPointPrecision.HALF) => StructField(name, FloatType, nullable = nullable)
        case i: ArrowType.FloatingPoint if i.getPrecision.equals(FloatingPointPrecision.SINGLE) => StructField(name, FloatType, nullable = nullable)
        case i: ArrowType.FloatingPoint if i.getPrecision.equals(FloatingPointPrecision.DOUBLE) => StructField(name, DoubleType, nullable = nullable)
        case i: ArrowType.Null => StructField(name, NullType, nullable = nullable)
        case i: ArrowType.Decimal => StructField(name, DecimalType(i.getPrecision, i.getScale), nullable = nullable)
        case i: ArrowType.Utf8 => StructField(name, StringType, nullable = nullable)
        case i: ArrowType.Date => StructField(name, DateType, nullable = nullable)
        case i: ArrowType.Timestamp => StructField(name, TimestampType, nullable = nullable)
        case _ => throw new IllegalArgumentException(s"unsupported type: ${fieldType.getType}")
      }
    }).toList
    StructType(fields = fields)
  }

  override def capabilities(): util.Set[TableCapability] = CollectionConverters.SetHasAsJava(Set(TableCapability.BATCH_READ)).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new DataSourceScanBuilder(config, flightInfo, schema)
}
