package com.grant.datasource.connector.spark.sql

import com.grant.datasource.connector.spark.sql.conf.DatastoreConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

class DataSourcePartitionReaderFactory(val config: DatastoreConfig) extends PartitionReaderFactory{

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = throw new UnsupportedOperationException("createReader is not supported")

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = new DataSourceColumnarBatchPartitionReader(config, partition.asInstanceOf[DataSourceInputPartition])
}
