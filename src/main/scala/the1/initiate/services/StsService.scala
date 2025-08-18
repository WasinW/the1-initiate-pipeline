package the1.initiate.services

import com.google.cloud.storage.transfer.v1.StorageTransferServiceClient
import com.google.cloud.secretmanager.v1.{SecretManagerServiceClient, SecretVersionName}
import com.google.storagetransfer.v1.proto.TransferTypes._
import com.google.storagetransfer.v1.proto._
import com.google.type.{Date => ProtoDate, TimeOfDay}
import com.google.longrunning.Operation
import java.time.{OffsetDateTime, ZoneOffset}
import scala.util.{Try, Success, Failure}
import scala.concurrent.duration._
import the1.initiate.logging.GcsLogger

/**
 * Service for handling Storage Transfer Service operations
 * with proper monitoring and error handling
 */
class StsService(projectId: String, logger: GcsLogger) {
  
  private val stsClient = StorageTransferServiceClient.create()
  private val secretClient = SecretManagerServiceClient.create()
  
  /**
   * Execute STS transfer from S3 to GCS with monitoring
   * @return Transfer job name if successful
   */
  def executeTransfer(
    sourceBucket: String,
    sourcePrefix: String,
    destBucket: String,
    destPrefix: String,
    awsKeySecretName: String = "aws-access-key-id",
    awsSecretSecretName: String = "aws-secret-access-key"
  ): Try[String] = {
    
    Try {
      logger.info(s"Starting STS transfer: s3://$sourceBucket/$sourcePrefix -> gs://$destBucket/$destPrefix")
      
      // 1. Retrieve AWS credentials
      val awsAccessKey = getSecretValue(awsKeySecretName)
      val awsSecretKey = getSecretValue(awsSecretSecretName)
      
      // 2. Build AWS source
      val awsSource = AwsS3Data.newBuilder()
        .setBucketName(sourceBucket)
        .setPath(if (sourcePrefix.nonEmpty) sourcePrefix else "")
        .setAwsAccessKey(
          AwsAccessKey.newBuilder()
            .setAccessKeyId(awsAccessKey)
            .setSecretAccessKey(awsSecretKey)
            .build()
        )
        .build()
      
      // 3. Build GCS destination
      val gcsSink = GcsData.newBuilder()
        .setBucketName(destBucket)
        .setPath(destPrefix)
        .build()
      
      // 4. Configure transfer options
      val transferOptions = TransferOptions.newBuilder()
        .setOverwriteWhenDifferent(true)  // Idempotent
        .setDeleteObjectsFromSourceAfterTransfer(false)
        .setDeleteObjectsUniqueInSink(false)
        .build()
      
      // 5. Object conditions (filter by prefix)
      val objectConditions = ObjectConditions.newBuilder()
        .addIncludePrefixes(sourcePrefix)
        .build()
      
      // 6. Build transfer spec
      val transferSpec = TransferSpec.newBuilder()
        .setAwsS3DataSource(awsSource)
        .setGcsDataSink(gcsSink)
        .setObjectConditions(objectConditions)
        .setTransferOptions(transferOptions)
        .build()
      
      // 7. Create one-time schedule (run immediately)
      val now = OffsetDateTime.now(ZoneOffset.UTC)
      val schedule = Schedule.newBuilder()
        .setScheduleStartDate(
          ProtoDate.newBuilder()
            .setYear(now.getYear)
            .setMonth(now.getMonthValue)
            .setDay(now.getDayOfMonth)
            .build()
        )
        .setScheduleEndDate(
          ProtoDate.newBuilder()
            .setYear(now.getYear)
            .setMonth(now.getMonthValue)
            .setDay(now.getDayOfMonth)
            .build()
        )
        .setStartTimeOfDay(
          TimeOfDay.newBuilder()
            .setHours(now.getHour)
            .setMinutes(now.getMinute)
            .setSeconds(now.getSecond)
            .build()
        )
        .build()
      
      // 8. Create transfer job
      val transferJob = TransferJob.newBuilder()
        .setProjectId(projectId)
        .setDescription(s"Data migration: $sourceBucket/$sourcePrefix to $destBucket/$destPrefix")
        .setTransferSpec(transferSpec)
        .setSchedule(schedule)
        .setStatus(TransferJob.Status.ENABLED)
        .build()
      
      val request = CreateTransferJobRequest.newBuilder()
        .setTransferJob(transferJob)
        .build()
      
      val createdJob = stsClient.createTransferJob(request)
      val jobName = createdJob.getName
      
      logger.info(s"Created STS job: $jobName")
      
      // 9. Monitor transfer progress
      monitorTransferJob(jobName)
      
      jobName
      
    } match {
      case Success(jobName) => 
        logger.info(s"STS transfer completed successfully: $jobName")
        Success(jobName)
      case Failure(e) =>
        logger.error(s"STS transfer failed", e)
        Failure(e)
    }
  }
  
  /**
   * Monitor transfer job until completion
   */
  private def monitorTransferJob(jobName: String): Unit = {
    logger.info(s"Monitoring transfer job: $jobName")
    
    var isComplete = false
    var checkCount = 0
    val maxChecks = 360  // 6 hours max (check every 60 seconds)
    
    while (!isComplete && checkCount < maxChecks) {
      Thread.sleep(60000)  // Wait 60 seconds between checks
      checkCount += 1
      
      try {
        val job = stsClient.getTransferJob(jobName)
        val latestOperation = job.getLatestOperationName
        
        if (latestOperation != null && !latestOperation.isEmpty) {
          // Check operation status
          val operationsClient = stsClient.getOperationsClient
          val operation = operationsClient.getOperation(latestOperation)
          
          if (operation.getDone) {
            isComplete = true
            if (operation.hasError) {
              throw new RuntimeException(s"Transfer operation failed: ${operation.getError.getMessage}")
            } else {
              logger.info(s"Transfer operation completed successfully")
              
              // Log transfer statistics if available
              if (operation.hasMetadata) {
                val metadata = operation.getMetadata
                logger.info(s"Transfer metadata: ${metadata.toString}")
              }
            }
          } else {
            logger.info(s"Transfer in progress... (check $checkCount/$maxChecks)")
          }
        }
      } catch {
        case e: Exception =>
          logger.warn(s"Error checking transfer status: ${e.getMessage}")
          // Continue monitoring unless it's a critical error
      }
    }
    
    if (!isComplete) {
      logger.warn(s"Transfer job monitoring timeout after $checkCount checks. Job may still be running.")
    }
  }
  
  /**
   * Retrieve secret value from Secret Manager
   */
  private def getSecretValue(secretName: String): String = {
    val secretVersion = SecretVersionName.of(projectId, secretName, "latest")
    val response = secretClient.accessSecretVersion(secretVersion)
    response.getPayload.getData.toStringUtf8.trim
  }
  
  /**
   * Clean up resources
   */
  def close(): Unit = {
    Try(stsClient.close())
    Try(secretClient.close())
  }
}