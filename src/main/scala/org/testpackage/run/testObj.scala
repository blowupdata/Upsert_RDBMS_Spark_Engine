package org.testpackage.run

import org.apache.spark.sql.SparkSession
import java.util.Properties

object testObj {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("TestApp").getOrCreate()
    val connectionProperties = new Properties();
    import spark.implicits._
    connectionProperties.put("user", "postgres");
    connectionProperties.put("password", "1234");
    connectionProperties.put("driver", "org.postgresql.Driver");

    var jdbcDF = spark.read.jdbc("jdbc:postgresql://localhost:5432/SparkDB", "fact_cust_info", connectionProperties)

    print(jdbcDF.show())
  }
}