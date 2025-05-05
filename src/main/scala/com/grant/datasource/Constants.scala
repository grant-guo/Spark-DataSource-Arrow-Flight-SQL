package com.grant.datasource

object Constants {
  val PREFIX = "spark.grant.datastore"
  val SOURCE_PREFIX = s"$PREFIX.source"
  val SINK_PREFIX = s"$PREFIX.sink"
  val KEY_DATABASE = "database"
  val KEY_TABLE = "table"
  val KEY_USERNAME = "username"
  val KEY_PASSWORD = "password"
  val KEY_ARROW_FLIGHT_URLS = "arrow.flight.urls"
  val KEY_QUERY = "query"
}
