package the1.initiate.services

import com.google.cloud.bigquery._
import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}
import the1.initiate.logging.GcsLogger

/**
 * Service for data validation between source and target
 */
class ValidationService(
  projectId: String,
  bigQueryService: BigQueryService,
  logger: GcsLogger
) {
  
  private val bigquery = BigQueryOptions.newBuilder()
    .setProjectId(projectId)
    .build()
    .getService
  
  case class ValidationResult(
    isValid: Boolean,
    sourceCount: Long,
    targetCount: Long,
    countMatch: Boolean,
    // checksumMatch: Option[Boolean] = None,
    checksumMatch: scala.Option[Boolean] = None,
    issues: Seq[String] = Seq.empty
  )
  
  /**
   * Validate data between external and managed tables
   */
  def validateTransfer(
    sourceDataset: String,
    sourceTable: String,
    targetDataset: String,
    targetTable: String,
    checksumColumns: Seq[String] = Seq.empty
  ): ValidationResult = {
    
    logger.info(s"Starting validation: $sourceDataset.$sourceTable vs $targetDataset.$targetTable")
    
    val issues = Seq.newBuilder[String]
    var sourceCount = 0L
    var targetCount = 0L
    
    // 1. Validate row counts
    val countValidation = for {
      srcCount <- bigQueryService.getRowCount(sourceDataset, s"${sourceTable}_ext")
      tgtCount <- bigQueryService.getRowCount(targetDataset, targetTable)
    } yield {
      sourceCount = srcCount
      targetCount = tgtCount
      
      if (srcCount != tgtCount) {
        issues += s"Row count mismatch: source=$srcCount, target=$tgtCount, diff=${Math.abs(srcCount - tgtCount)}"
      }
      srcCount == tgtCount
    }
    
    val countMatch = countValidation.getOrElse {
      issues += "Failed to get row counts"
      false
    }
    
    // 2. Validate checksums if columns specified
    val checksumMatch = if (checksumColumns.nonEmpty && countMatch) {
      validateChecksums(
        sourceDataset, s"${sourceTable}_ext",
        targetDataset, targetTable,
        checksumColumns
      ) match {
        // case Success((match, checkIssues)) =>
        //   issues ++= checkIssues
        //   Some(match)
        // case Failure(e) =>
        //   issues += s"Checksum validation failed: ${e.getMessage}"
        //   Some(false)
        case Success((isMatch, checkIssues)) =>
        issues ++= checkIssues
        Some(isMatch)
        case Failure(e) =>
        issues += s"Checksum validation failed: ${e.getMessage}"
        Some(false)

      }
    } else {
      None
    }
    
    // 3. Sample data validation (check for nulls in key columns)
    if (countMatch && checksumColumns.nonEmpty) {
      validateSampleData(targetDataset, targetTable, checksumColumns) match {
        case Success(sampleIssues) => issues ++= sampleIssues
        case Failure(e) => issues += s"Sample validation failed: ${e.getMessage}"
      }
    }
    
    val finalIssues = issues.result()
    val isValid = countMatch && checksumMatch.getOrElse(true) && finalIssues.isEmpty
    
    val result = ValidationResult(
      isValid = isValid,
      sourceCount = sourceCount,
      targetCount = targetCount,
      countMatch = countMatch,
      checksumMatch = checksumMatch,
      issues = finalIssues
    )
    
    // Log validation summary
    logValidationSummary(result)
    
    result
  }
  
  /**
   * Validate checksums for specified columns
   */
  private def validateChecksums(
    sourceDataset: String,
    sourceTable: String,
    targetDataset: String,
    targetTable: String,
    columns: Seq[String]
  ): Try[(Boolean, Seq[String])] = {
    
    Try {
      val issues = Seq.newBuilder[String]
      var allMatch = true
      
      columns.foreach { column =>
        val sourceChecksum = calculateChecksum(sourceDataset, sourceTable, column)
        val targetChecksum = calculateChecksum(targetDataset, targetTable, column)
        
        (sourceChecksum, targetChecksum) match {
          case (Success(srcSum), Success(tgtSum)) =>
            if (srcSum != tgtSum) {
              issues += s"Checksum mismatch for column '$column': source=$srcSum, target=$tgtSum"
              allMatch = false
            } else {
              logger.info(s"Checksum match for column '$column': $srcSum")
            }
          case (Failure(e), _) =>
            issues += s"Failed to calculate source checksum for '$column': ${e.getMessage}"
            allMatch = false
          case (_, Failure(e)) =>
            issues += s"Failed to calculate target checksum for '$column': ${e.getMessage}"
            allMatch = false
        }
      }
      
      (allMatch, issues.result())
    }
  }
  
  /**
   * Calculate checksum for a column
   */
  private def calculateChecksum(
    datasetId: String,
    tableName: String,
    column: String
  ): Try[String] = {
    
    val query = s"""
      |SELECT 
      |  TO_BASE64(MD5(STRING_AGG(CAST($column AS STRING), ',' ORDER BY $column))) as checksum
      |FROM `$projectId.$datasetId.$tableName`
      |WHERE $column IS NOT NULL
    """.stripMargin
    
    Try {
      val queryConfig = QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(false)
        .build()
      
      val queryJob = bigquery.create(JobInfo.of(queryConfig))
      val result = queryJob.waitFor().getQueryResults()
      
      result.iterateAll().asScala.toSeq.headOption
        .flatMap(row => scala.Option(row.get("checksum")))
        .map(_.getStringValue)
        .getOrElse("")
    }
  }
  
  /**
   * Validate sample data for quality issues
   */
  private def validateSampleData(
    datasetId: String,
    tableName: String,
    keyColumns: Seq[String]
  ): Try[Seq[String]] = {
    
    Try {
      val issues = Seq.newBuilder[String]
      
      // Check for nulls in key columns
      keyColumns.foreach { column =>
        val nullCountQuery = s"""
          |SELECT COUNT(*) as null_count
          |FROM `$projectId.$datasetId.$tableName`
          |WHERE $column IS NULL
        """.stripMargin
        
        val queryConfig = QueryJobConfiguration.newBuilder(nullCountQuery)
          .setUseLegacySql(false)
          .build()
        
        val queryJob = bigquery.create(JobInfo.of(queryConfig))
        val result = queryJob.waitFor().getQueryResults()
        
        val nullCount = result.iterateAll().asScala.toSeq.head
          .get("null_count")
          .getLongValue
        
        if (nullCount > 0) {
          issues += s"Found $nullCount NULL values in key column '$column'"
        }
      }
      
      // Check for duplicates on primary key (assuming first column is PK)
      if (keyColumns.nonEmpty) {
        val pkColumn = keyColumns.head
        val dupQuery = s"""
          |SELECT $pkColumn, COUNT(*) as cnt
          |FROM `$projectId.$datasetId.$tableName`
          |GROUP BY $pkColumn
          |HAVING COUNT(*) > 1
          |LIMIT 10
        """.stripMargin
        
        val queryConfig = QueryJobConfiguration.newBuilder(dupQuery)
          .setUseLegacySql(false)
          .build()
        
        val queryJob = bigquery.create(JobInfo.of(queryConfig))
        val result = queryJob.waitFor().getQueryResults()
        
        val duplicates = result.iterateAll().asScala.toSeq
        if (duplicates.nonEmpty) {
          val dupCount = duplicates.size
          issues += s"Found $dupCount duplicate values in primary key column '$pkColumn'"
        }
      }
      
      issues.result()
    }
  }
  
  /**
   * Log validation summary
   */
  private def logValidationSummary(result: ValidationResult): Unit = {
    val summary = s"""
      |==================================================
      |VALIDATION SUMMARY
      |==================================================
      |Valid: ${result.isValid}
      |Source Count: ${result.sourceCount}
      |Target Count: ${result.targetCount}
      |Count Match: ${result.countMatch}
      |Checksum Match: ${result.checksumMatch.map(_.toString).getOrElse("N/A")}
      |Issues: ${if (result.issues.isEmpty) "None" else result.issues.mkString("\n  - ")}
      |==================================================
    """.stripMargin
    
    if (result.isValid) {
      logger.info(summary)
    } else {
      logger.error(summary)
    }
  }
}