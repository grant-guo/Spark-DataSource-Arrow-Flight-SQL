package com.grant.datasource.connector.spark.sql.conf

import com.grant.datasource.Constants.{KEY_ARROW_FLIGHT_URLS, KEY_DATABASE, KEY_PASSWORD, KEY_QUERY, KEY_TABLE, KEY_USERNAME}

case class DatastoreConfig(arrowFlightUrls: Array[String], user: String, password: String, database: String, tableOrQuery: Either[String, String])

object DatastoreConfig {
  def of(conf: Map[String, String]): DatastoreConfig = {
    val tableOrQuery = (conf.get(KEY_TABLE), conf.get(KEY_QUERY)) match {
      case (Some(table), None) => Left(table)
      case (None, Some(query)) => Right(query)
      case (Some(_), Some(_)) => throw new IllegalArgumentException("Either table or query must be specified, not both")
      case _ => throw new IllegalArgumentException("Either table or query must be specified")
    }

    val database = conf.getOrElse(KEY_DATABASE, throw new IllegalArgumentException("Database must be specified"))

    val (user, password) = (conf.get(KEY_USERNAME), conf.get(KEY_PASSWORD)) match {
      case (Some(username), Some(password)) => (username, password)
      case _ => throw new IllegalArgumentException("Username and password must be specified")
    }

    val arrowFlightUrls = conf.getOrElse(KEY_ARROW_FLIGHT_URLS, throw new IllegalArgumentException("Arrow Flight URLs must be specified")).split(",").map(_.trim)

    DatastoreConfig(
      arrowFlightUrls = arrowFlightUrls,
      user = user,
      password = password,
      database = database,
      tableOrQuery = tableOrQuery
    )
  }


}
