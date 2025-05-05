package com.grant.datasource.connector.spark.sql.res

import org.apache.arrow.memory.RootAllocator

object ResourceAllocator {
  lazy val allocator = new RootAllocator(Integer.MAX_VALUE)
}
