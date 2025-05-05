package com.grant.datasource.connector.spark.sql

import com.grant.datasource.connector.spark.sql.conf.DatastoreConfig
import org.apache.spark.sql.connector.read.InputPartition

import java.net.URI

class DataSourceInputPartition(val config: DatastoreConfig, val location: URI, val ticket: Array[Byte]) extends InputPartition{

}
