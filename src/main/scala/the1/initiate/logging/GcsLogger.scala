package the1.initiate.logging

import com.google.cloud.storage.{BlobId, BlobInfo, Storage, StorageOptions}
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.nio.charset.StandardCharsets
import java.util.ArrayList  // ⭐ ใช้ Java ArrayList
import scala.collection.JavaConverters._  // ⭐ สำหรับ convert

class GcsLogger(
  bucketName: String = "demo-central-the1",
  logPrefix: String = "data-platform/logs"
) {
  
  private val storage: Storage = StorageOptions.getDefaultInstance.getService
  private val logs = new ArrayList[String]()  // ⭐ ใช้ Java ArrayList
  private val startTime = LocalDateTime.now()
  private val sessionId = s"${startTime.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))}_${System.currentTimeMillis}"
  
  def info(message: String): Unit = {
    log("INFO", message)
  }
  
  def warn(message: String): Unit = {
    log("WARN", message)
  }
  
  def error(message: String, throwable: Throwable = null): Unit = {
    val errorMsg = if (throwable != null) {
      s"$message\n${throwable.getClass.getName}: ${throwable.getMessage}\n${throwable.getStackTrace.take(10).mkString("\n")}"
    } else {
      message
    }
    log("ERROR", errorMsg)
  }
  
  private def log(level: String, message: String): Unit = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    val logLine = s"[$timestamp] [$level] $message"
    
    // Console output
    level match {
      case "ERROR" => System.err.println(logLine)
      case _ => println(logLine)
    }
    
    // Store for GCS - ใช้ Java add() method
    logs.add(logLine)  // ⭐ เปลี่ยนจาก += เป็น add()
  }
  
  def flush(tableName: String = "general"): Unit = {
    if (!logs.isEmpty) {  // ⭐ ใช้ isEmpty() ของ Java
      try {
        val date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy/MM/dd"))
        val fileName = s"$logPrefix/$date/${tableName}_$sessionId.log"
        
        val blobId = BlobId.of(bucketName, fileName)
        val blobInfo = BlobInfo.newBuilder(blobId)
          .setContentType("text/plain")
          .build()
        
        // Convert Java ArrayList to Scala และ join
        val content = logs.asScala.mkString("\n").getBytes(StandardCharsets.UTF_8)  // ⭐ convert ด้วย asScala
        storage.create(blobInfo, content)
        
        println(s"Logs written to: gs://$bucketName/$fileName")
        logs.clear()  // ⭐ ใช้ clear() ของ Java
      } catch {
        case e: Exception =>
          System.err.println(s"Failed to write logs to GCS: ${e.getMessage}")
      }
    }
  }
  
  def writeSummary(
    tableName: String,
    status: String,
    rowsTransferred: Long = 0,
    duration: String = "",
    errors: Seq[String] = Seq.empty
  ): Unit = {
    val summary = s"""
      |==================================================
      |EXECUTION SUMMARY
      |==================================================
      |Table: $tableName
      |Status: $status
      |Start Time: ${startTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}
      |End Time: ${LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}
      |Duration: $duration
      |Rows Transferred: $rowsTransferred
      |Errors: ${if (errors.isEmpty) "None" else errors.mkString("\n  - ")}
      |==================================================
    """.stripMargin
    
    info(summary)
    flush(tableName)
  }
}