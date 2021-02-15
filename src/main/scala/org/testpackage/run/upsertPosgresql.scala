package org.testpackage.run

import org.apache.spark.sql.SparkSession
import java.util.Properties
import java.sql.DriverManager
import java.sql.Connection
import java.sql.PreparedStatement

object upsertPosgresql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("TestApp").getOrCreate()
    val connectionProperties = new Properties();

    import spark.implicits._
    val jdbcurl = "jdbc:postgresql://localhost:5432/SparkDB"
    val user = "postgres"
    val password = "1234"
    val jdbcdriver = "org.postgresql.Driver"
    connectionProperties.put("user", user);
    connectionProperties.put("password", password);
    connectionProperties.put("driver", jdbcdriver);
    connectionProperties.put("jdbcUrl", jdbcurl)
    val updDf = spark
    .sql("""select 'TRUMP' as Name,'USA' as Country 
      union all 
      select 'CHANDRU' as Name, 'INDIA' as Country""")
    print(updDf.show())
    val sc = spark.sparkContext
    val br_conn_props = sc.broadcast(connectionProperties)
    updDf.coalesce(5).foreachPartition(partition => {
      val connectionProperties = br_conn_props.value
      val jdbcurl = connectionProperties.getProperty("jdbcUrl")
      val user = connectionProperties.getProperty("user")
      val password = connectionProperties.getProperty("password")
      val driver = connectionProperties.getProperty("driver")
      Class.forName(driver)
      val dbc: Connection = DriverManager.getConnection(jdbcurl, user, password)
      val db_batch_size = 10
      var st: PreparedStatement = null
      partition.grouped(db_batch_size).foreach(batch => {
        batch.foreach(row => {
          val custName = row.fieldIndex("Name")
          val customerName = row.getString(custName)
          val custCountry = row.fieldIndex("Country")
          val customerCountry = row.getString(custCountry)
          val sqlstring = s"""Select "Name","Country" from fact_cust_info where "Name"='$customerName'"""

          var pstmt: PreparedStatement = dbc.prepareStatement(sqlstring)
          val rs = pstmt.executeQuery()
          var count = 0
          while (rs.next()) {
            count = 1
          }
          var dmloprtn = "NULL"
          if (count > 0)
            dmloprtn = "UPDATE"
          else
            dmloprtn = "INSERT"

          if (dmloprtn == "UPDATE") {
            val updateString = s"""UPDATE fact_cust_info set "Country"='$customerCountry' where "Name"='$customerName'"""
            st = dbc.prepareStatement(updateString)
          } else if (dmloprtn == "INSERT") {
            val insertString = s"""INSERT INTO fact_cust_info ("Name","Country") values('$customerName','$customerCountry')"""
            st = dbc.prepareStatement(insertString)

          }
          st.addBatch()
        })
        st.executeBatch()
      })

      dbc.close()
    })
println("Job is complete")
  }
}