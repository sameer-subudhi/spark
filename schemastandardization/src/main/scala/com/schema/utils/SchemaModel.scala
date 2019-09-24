package com.schema.utils

import Utils._
import java.sql.{Connection, DriverManager}

class SchemaModel {

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://<server>:<port>/<db>"
  val username = "<user>"
  val password = "<password>"
  var connection: Connection = null

  def getSchemaFromDB(schemaName: String, tableName: String)
  : List[(String, String)] = {
    try {
      Class.forName("org.postgresql.Driver")
      connection = DriverManager.getConnection(url, username, password)
     // println("connection" + connection)
      val statement = connection.createStatement()
     // println("statement" + statement)
      val command = "select column_name, data_type from INFORMATION_SCHEMA.columns where table_name = '" +
        tableName + "' and table_schema = '" + schemaName + "' order by ordinal_position"
     // println(command)
      val resultSet = statement.executeQuery(command)
     // println("result" + resultSet)
      var schema = List[(String, String)]()
      while (resultSet.next()) {
        val columnName = resultSet.getString("column_name")
        val dataType = resultSet.getString("data_type")
        schema :+= (toLowerCamelCase(columnName), dataType)
      }
      schema
    }
    catch {
      case e: Exception => throw e
    }
    finally {
      connection.close()
    }

  }
}