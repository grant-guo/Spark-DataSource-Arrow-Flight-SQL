package com.grant.datasource.connector.spark.sql

import com.grant.datasource.connector.spark.sql.conf.DatastoreConfig
import org.apache.arrow.flight.FlightInfo
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

class DataSourceScanBuilder(val config: DatastoreConfig, val flightInfo: FlightInfo, val schema: StructType) extends ScanBuilder{

  override def build(): Scan = new DataSourceScan(config, flightInfo, schema)
}
