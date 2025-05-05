package com.grant.datasource.connector.spark.sql

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class DataSourceTableProvider extends TableProvider{

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // the purpose of this function is to infer the schema from the configurations passed through df.option(..).options(...)
    null
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = ???
}
