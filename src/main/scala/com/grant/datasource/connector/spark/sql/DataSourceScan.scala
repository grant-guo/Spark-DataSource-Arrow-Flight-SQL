package com.grant.datasource.connector.spark.sql

import com.grant.datasource.connector.spark.sql.conf.DatastoreConfig
import org.apache.arrow.flight.FlightInfo
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters

class DataSourceScan(val config: DatastoreConfig, val flightInfo: FlightInfo, val schema: StructType) extends Scan with Batch{

  override def readSchema(): StructType = this.schema

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val array = CollectionConverters.ListHasAsScala(flightInfo.getEndpoints).asScala.flatMap(endpoint => {
      CollectionConverters.ListHasAsScala(endpoint.getLocations).asScala.map(location => new DataSourceInputPartition(config, location.getUri, endpoint.getTicket.getBytes))
    }).map(_.asInstanceOf[InputPartition]).toArray
    println(s"${array.length} partitions will be created")
    array
  }

  override def createReaderFactory(): PartitionReaderFactory = ???
}
