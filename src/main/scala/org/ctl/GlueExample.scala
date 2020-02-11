package org.ctl

import java.util

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.client.AwsSyncClientParams
import com.amazonaws.handlers.RequestHandler2
import com.amazonaws.metrics.RequestMetricCollector
import com.amazonaws.monitoring.{CsmConfigurationProvider, MonitoringListener}
import com.amazonaws.services.glue.{AWSGlue, AWSGlueClient}
import com.amazonaws.services.glue.model.{GetPartitionRequest, GetPartitionsRequest, GetTableRequest, SearchTablesRequest}

import scala.collection.JavaConverters._

object GlueExample {

  def main(args: Array[String]):Unit = {

    case class CatColumn(name: String, dataType: String)
    case class CatTable(name: String, database: String, column: List[CatColumn])

    val credentials = new AWSCredentials {
      def getAWSAccessKeyId: String = EnvUtil.envVarOrError("LOAD_FILE_ACCESS_KEY")
      def getAWSSecretKey: String = EnvUtil.envVarOrError("LOAD_FILE_SECRET_KEY")
    }
    val provider = new AWSCredentialsProvider {
      override def getCredentials: AWSCredentials = credentials
      override def refresh(): Unit = ???
    }

    val client: AWSGlue =
      AWSGlueClient
        .builder()
        .withCredentials(provider)
        .build()

//    val table = client.getTable(new GetTableRequest()
//      .withDatabaseName("database")
//      .withCatalogId("315028976277")
//      .withName("tablename"))
//    println(table)

    val searchTablesRequest = {
      val request = new SearchTablesRequest()
      request.setCatalogId("NDW")
      //request.setMaxResults(1000000)
      request
    }
    val tables = client.searchTables(searchTablesRequest)

    val catTables =
      tables.getTableList.asScala.map(table => {
        val databaseName = table.getDatabaseName
        val tableName = table.getName
        // ??? val location = table.getStorageDescriptor.getLocation
        val columns = table.getStorageDescriptor.getColumns

        val partitionColumns = table.getPartitionKeys
        val dateColumn = partitionColumns.asScala.filter(_.getType == "date").head
        //dateColumn.


        val parts =
          client.getPartitions({
            val req = new GetPartitionsRequest()
            req.setDatabaseName(databaseName)
            req.setTableName(tableName)
            //req.setCatalogId()
            req
          })
        val part = parts.getPartitions.asScala.head
        part.getValues().asScala.foreach(v => println(v))

        val catColumns =
          columns.asScala.map(column => {
            val columnName = column.getName
            val dataType = column.getType
            CatColumn(columnName, dataType)
          })

        val output = CatTable(tableName, databaseName, catColumns.toList)
        println("Table: " + output)
        output
      })
  }
}





