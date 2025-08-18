package the1.initiate.services

// ⭐ Import ที่ถูกต้องตาม Solution
import com.google.storagetransfer.v1.proto.{
  StorageTransferServiceClient,
  TransferProto,
  TransferTypes
}
import com.google.cloud.secretmanager.v1.{SecretManagerServiceClient, SecretVersionName}
import com.google.protobuf.Empty
import com.google.protobuf.util.Timestamps
import com.google.`type`.{Date => ProtoDate, TimeOfDay => ProtoTimeOfDay}

import java.time.{OffsetDateTime, ZoneOffset}
import scala.util.{Try, Success, Failure}
import scala.concurrent.duration._
import the1.initiate.logging.GcsLogger

/**
 * Updated STS Service with correct imports and API usage
 */
class StsService(projectId: String, logger: GcsLogger) {
  
  private val stsClient = StorageTransferServiceClient.create()
  private val secretClient = SecretManagerServiceClient.create()
  
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
      
      // 1. Get AWS credentials
      val awsAccessKey = getSecretValue(awsKeySecretName)
      val awsSecretKey = getSecretValue(awsSecretSecretName)
      
      // 2. Build transfer specification using correct types
      val awsSource = TransferTypes.AwsS3Data.newBuilder()
        .setBucketName(sourceBucket)
        .setPath(normalizePrefix(sourcePrefix))
        .setAwsAccessKey(
          TransferTypes.AwsAccessKey.newBuilder()
            .setAccessKeyId(awsAccessKey)
            .setSecretAccessKey(awsSecretKey)
            .build()
        )
        .build()
      
      val gcsSink = TransferTypes.GcsData.newBuilder()
        .setBucketName(destBucket)
        .setPath(normalizePrefix(destPrefix))
        .build()
      
      val transferOptions = TransferTypes.TransferOptions.newBuilder()
        .setOverwriteObjectsAlreadyExistingInSink(true)
        .setDeleteObjectsFromSourceAfterTransfer(false)
        .setDeleteObjectsUniqueInSink(false)
        .build()
      
      val transferSpec = TransferTypes.TransferSpec.newBuilder()
        .setAwsS3DataSource(awsSource)
        .setGcsDataSink(gcsSink)
        .setTransferOptions(transferOptions)
        .build()
      
      // 3. Schedule for immediate run
      val now = OffsetDateTime.now(ZoneOffset.UTC)
      val schedule = TransferTypes.Schedule.newBuilder()
        .setScheduleStartDate(
          ProtoDate.newBuilder()
            .setYear(now.getYear)
            .setMonth(now.getMonthValue)
            .setDay(now.getDayOfMonth)
        )
        .setScheduleEndDate(
          ProtoDate.newBuilder()
            .setYear(now.getYear)
            .setMonth(now.getMonthValue)
            .setDay(now.getDayOfMonth)
        )
        .build()
      
      // 4. Create transfer job
      val transferJob = TransferTypes.TransferJob.newBuilder()
        .setProjectId(projectId)
        .setDescription(s"Migration: $sourceBucket/$sourcePrefix")
        .setTransferSpec(transferSpec)
        .setSchedule(schedule)
        .setStatus(TransferTypes.TransferJob.Status.ENABLED)
        .build()
      
      // 5. Submit job using correct request type
      val createRequest = TransferProto.CreateTransferJobRequest.newBuilder()
        .setTransferJob(transferJob)
        .build()
      
      val response = stsClient.createTransferJob(createRequest)
      val jobName = response.getName
      
      logger.info(s"Created transfer job: $jobName")
      
      // 6. Run immediately
      val runRequest = TransferProto.RunTransferJobRequest.newBuilder()
        .setJobName(jobName)
        .setProjectId(projectId)
        .build()
      
      val operation = stsClient.runTransferJobCallable().call(runRequest)
      logger.info(s"Started transfer operation: ${operation.getName}")
      
      // 7. Simple monitoring (production ควร improve)
      Thread.sleep(5000) // Wait 5 seconds
      
      // 8. Check status
      val getRequest = TransferProto.GetTransferJobRequest.newBuilder()
        .setJobName(jobName)
        .setProjectId(projectId)
        .build()
      
      val job = stsClient.getTransferJob(getRequest)
      logger.info(s"Job status: ${job.getStatus}")
      
      jobName
    }
  }
  
  private def normalizePrefix(prefix: String): String = {
    if (prefix.nonEmpty && !prefix.endsWith("/")) s"$prefix/" else prefix
  }
  
  private def getSecretValue(secretName: String): String = {
    val secretVersion = SecretVersionName.of(projectId, secretName, "latest")
    val response = secretClient.accessSecretVersion(secretVersion)
    response.getPayload.getData.toStringUtf8.trim
  }
  
  def close(): Unit = {
    Try(stsClient.close())
    Try(secretClient.close())
  }
}