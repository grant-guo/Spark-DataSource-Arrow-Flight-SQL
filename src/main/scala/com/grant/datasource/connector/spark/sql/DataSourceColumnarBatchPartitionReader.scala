package com.grant.datasource.connector.spark.sql

import com.grant.datasource.connector.spark.sql.conf.DatastoreConfig
import com.grant.datasource.connector.spark.sql.res.ResourceAllocator
import com.grant.datasource.connector.spark.sql.res.ResourceAllocator.allocator
import org.apache.arrow.flight.grpc.CredentialCallOption
import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.flight.{FlightClient, Location, Ticket}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import scala.jdk.CollectionConverters

class DataSourceColumnarBatchPartitionReader(val config: DatastoreConfig, val partition: DataSourceInputPartition) extends PartitionReader[ColumnarBatch]{

  private lazy val (sqlStream, beSqlClient) = {
    println(s"location: ${partition.location}, ticket: ${partition.ticket}")

    val credentials = config.arrowFlightUrls.foldLeft(Option[CredentialCallOption](null))((option, url) => {
      option match {
        case Some(_) => option
        case None => {
          val urlRegex = "(.+):([0-9]+)".r
          url match {
            case urlRegex(host, port) => {
              val feLocation = Location.forGrpcInsecure(host, port.toInt)
              val feClient = FlightClient.builder(allocator, feLocation).build()
              val ret = Some(feClient.authenticateBasicToken(config.user, config.password).get())
              feClient.close()
              ret
            }
            case _ => None
          }
        }
      }
    }) match {
      case Some(credentialCallOption) => credentialCallOption
      case None => throw new IllegalArgumentException("failed to retrieve the flight endpoints from the fe.arrow.flight.url")
    }

    val beClient = FlightClient.builder(ResourceAllocator.allocator, new Location(partition.location)).build()
    val beSqlClient = new FlightSqlClient(beClient)
    (beSqlClient.getStream(new Ticket(partition.ticket), credentials), beSqlClient)
  }

  override def next(): Boolean = sqlStream.next()

  override def get(): ColumnarBatch = {
    val root = sqlStream.getRoot
    new ColumnarBatch(
      CollectionConverters.ListHasAsScala(root.getFieldVectors).asScala.map(vector => new ArrowColumnVector(vector)).toArray,
      root.getRowCount
    )
  }

  override def close(): Unit = {
    sqlStream.close()
    beSqlClient.close()
  }
}
