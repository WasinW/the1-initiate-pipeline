package the1.initiate.services

import com.google.cloud.bigquery._
import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}
import the1.initiate.logging.GcsLogger
import com.google.cloud.bigquery.JobStatistics.QueryStatistics

/**
 * Service for handling all BigQuery operations
 */
class BigQueryService(projectId: String, logger: GcsLogger) {
  
  private val bigquery = BigQueryOptions.newBuilder()
    .setProjectId(projectId)
    .build()
    .getService
  
  /**
   * Create or refresh external table pointing to GCS
   */
  def createOrRefreshExternalTable(
    datasetId: String,
    tableName: String,
    gcsBucket: String,
    gcsPrefix: String,
    format: String = "PARQUET",
    connectionId: String,
    region: String = "asia-southeast1"
  ): Try[Unit] = {
    
    val externalTableName = s"${tableName}_ext"
    val gcsUri = s"gs://$gcsBucket/$gcsPrefix*"
    
    val ddl = s"""
      |CREATE OR REPLACE EXTERNAL TABLE `$projectId.$datasetId.$externalTableName`
      |OPTIONS (
      |  format = '${format.toUpperCase}',
      |  uris = ['$gcsUri']
      |)
    """.stripMargin
    
    logger.info(s"Creating external table: $datasetId.$externalTableName")
    logger.info(s"DDL: $ddl")
    
    executeDDL(ddl)
  }
  
  /**
   * Create managed Iceberg table
   */
  def createManagedTable(
    datasetId: String,
    tableName: String,
    schema: String,
    gcsBucket: String,
    storagePrefix: String,
    connectionId: String,
    region: String = "asia-southeast1"
  ): Try[Unit] = {
    
    val ddl = s"""
      |CREATE TABLE IF NOT EXISTS `$projectId.$datasetId.$tableName` (
      |$schema
      |)
      |WITH CONNECTION `$region.$connectionId`
      |OPTIONS (
      |  file_format = 'PARQUET',
      |  table_format = 'ICEBERG',
      |  storage_uri = 'gs://$gcsBucket/$storagePrefix'
      |)
    """.stripMargin
    
    logger.info(s"Creating managed table: $datasetId.$tableName")
    logger.info(s"DDL: $ddl")
    
    executeDDL(ddl)
  }
  
  /**
   * Load data from external table to managed table
   */
  def loadIntoManagedTable(
    sourceDataset: String,
    sourceTable: String,
    targetDataset: String,
    targetTable: String,
    columnExpression: String,
    mode: String = "APPEND"  // APPEND, TRUNCATE, or MERGE
  ): Try[Long] = {
    
    val sourceFullName = s"`$projectId.$sourceDataset.${sourceTable}_ext`"
    val targetFullName = s"`$projectId.$targetDataset.$targetTable`"
    
    val dml = mode.toUpperCase match {
      case "TRUNCATE" =>
        s"""
          |TRUNCATE TABLE $targetFullName;
          |INSERT INTO $targetFullName
          |SELECT $columnExpression
          |FROM $sourceFullName
        """.stripMargin
        
      case "MERGE" =>
        // Assuming MEMBER_ID is the primary key
        s"""
          |MERGE $targetFullName T
          |USING (
          |  SELECT $columnExpression
          |  FROM $sourceFullName
          |) S
          |ON T.member_id = S.member_id
          |WHEN MATCHED THEN
          |  UPDATE SET *
          |WHEN NOT MATCHED THEN
          |  INSERT *
        """.stripMargin
        
      case _ =>  // APPEND (default)
        s"""
          |INSERT INTO $targetFullName
          |SELECT $columnExpression
          |FROM $sourceFullName
        """.stripMargin
    }
    
    logger.info(s"Loading data from $sourceDataset.$sourceTable to $targetDataset.$targetTable")
    logger.info(s"Mode: $mode")
    logger.info(s"DML: $dml")
    
    executeDML(dml)
  }
  
  /**
   * Execute DDL statement
   */
  private def executeDDL(ddl: String): Try[Unit] = {
    Try {
      val queryConfig = QueryJobConfiguration.newBuilder(ddl)
        .setUseLegacySql(false)
        .build()
      
      val queryJob = bigquery.create(JobInfo.of(queryConfig))
      val completedJob = queryJob.waitFor()
      
      if (completedJob == null) {
        throw new RuntimeException("Job no longer exists")
      } else if (completedJob.getStatus.getError != null) {
        val error = completedJob.getStatus.getError
        throw new RuntimeException(s"Query failed: ${error.getMessage}")
      } else {
        logger.info("DDL executed successfully")
      }
    } match {
      case Success(_) => Success(())
      case Failure(e) =>
        logger.error(s"DDL execution failed", e)
        Failure(e)
    }
  }
  
  /**
   * Execute DML statement and return affected rows
   */
  private def executeDML(dml: String): Try[Long] = {
    Try {
      val queryConfig = QueryJobConfiguration.newBuilder(dml)
        .setUseLegacySql(false)
        .build()
      
      val queryJob = bigquery.create(JobInfo.of(queryConfig))
      val completedJob = queryJob.waitFor()
      
      if (completedJob == null) {
        throw new RuntimeException("Job no longer exists")
      } else if (completedJob.getStatus.getError != null) {
        val error = completedJob.getStatus.getError
        throw new RuntimeException(s"Query failed: ${error.getMessage}")
      } else {
        val stats = completedJob.getStatistics.asInstanceOf[QueryStatistics]
        // val rowsAffected = Option(stats.getNumDmlAffectedRows).map(_.longValue()).getOrElse(0L)
        // ใช้ scala.Option แทน
        val rowsAffected = scala.Option(stats.getNumDmlAffectedRows).map(_.longValue()).getOrElse(0L)
        logger.info(s"DML executed successfully. Rows affected: $rowsAffected")
        rowsAffected
      }
    } match {
      case Success(rows) => Success(rows)
      case Failure(e) =>
        logger.error(s"DML execution failed", e)
        Failure(e)
    }
  }
  
  /**
   * Get row count from a table
   */
  def getRowCount(datasetId: String, tableName: String): Try[Long] = {
    val query = s"SELECT COUNT(*) as cnt FROM `$projectId.$datasetId.$tableName`"
    
    Try {
      val queryConfig = QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(false)
        .build()
      
      val queryJob = bigquery.create(JobInfo.of(queryConfig))
      val result = queryJob.waitFor().getQueryResults()
      
      result.iterateAll().asScala.toSeq.head
        .get("cnt")
        .getLongValue
    } match {
      case Success(count) =>
        logger.info(s"Row count for $datasetId.$tableName: $count")
        Success(count)
      case Failure(e) =>
        logger.error(s"Failed to get row count", e)
        Failure(e)
    }
  }
  
  /**
   * Check if table exists
   */
  def tableExists(datasetId: String, tableName: String): Boolean = {
    Try {
      val tableId = TableId.of(projectId, datasetId, tableName)
      val table = bigquery.getTable(tableId)
      table != null
    }.getOrElse(false)
  }
  
  /**
   * Delete table if exists
   */
  def deleteTableIfExists(datasetId: String, tableName: String): Try[Unit] = {
    if (tableExists(datasetId, tableName)) {
      Try {
        val tableId = TableId.of(projectId, datasetId, tableName)
        bigquery.delete(tableId)
        logger.info(s"Deleted table: $datasetId.$tableName")
      }
    } else {
      Success(())
    }
  }
}