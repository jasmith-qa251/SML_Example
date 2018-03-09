package com.example.sparktesting

import java.io.{File, IOException}
import java.net.BindException
import java.util.UUID

import org.apache.spark.{SparkConf, SparkException}

import scala.collection.mutable
import scala.reflect.ClassTag

object Utils {

  private val shutDownDeletePaths = new mutable.HashSet[String]()

  Runtime.getRuntime.addShutdownHook(new Thread("Delete Spark Tem Dirs") {
    override def run(): Unit = {
      shutDownCleanUp()
    }
  })

  def shutDownCleanUp(): Unit = {
    shutDownDeletePaths.foreach(cleanupPath)
  }

  private def cleanupPath(dirPath: String) = {
    try {
      Utils.deleteRecursively(new File(dirPath))
    } catch {
      case e: Exception => println("Exception During Cleanup")
    }
  }

  def isSymLink(file: File): Boolean = {
    if ( file == null ) throw new NullPointerException("File must not be Null")
    val fileInCanonicalDir = if (file.getParent == null) {
      file
    } else {
      new File(file.getParentFile().getCanonicalFile(), file.getName())
    }

    !fileInCanonicalDir.getCanonicalFile.equals(
      fileInCanonicalDir.getAbsoluteFile()
    )
  }

  private def listFilesSafely(file: File): Seq[File] = {
    if (file.exists()) {
      val files = file.listFiles()
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file)
      }
      files
    } else {
      List()
    }
  }

  def deleteRecursively(file: File) {
    if (file != null) {
      try {
        if (file.isDirectory && !isSymLink(file)) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
          shutDownDeletePaths.synchronized {
            shutDownDeletePaths.remove(file.getAbsolutePath)
          }
        }
      } finally  {
        if (!file.delete()) {
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  def registerShutdownDeleteDir(file:File): Unit = {
    val absolutePath = file.getAbsolutePath()
    shutDownDeletePaths.synchronized {
      shutDownDeletePaths += absolutePath
    }
  }

  def createDirectory(root: String): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException(
          s"Failed to create a temp directory (under ${root}) after ${maxAttempts}"
        )
      }
      try {
        dir = new File(root, "spark-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }
    dir
  }

  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
    val dir = createDirectory(root)
    registerShutdownDeleteDir(dir)
    dir
  }

  def startServiceOnPort[T](
                             startPort: Int,
                             startService: Int => (T, Int),
                             conf: SparkConf,
                             serviceName: String = ""): (T, Int) = {

    require(startPort == 0 || (1024 <= startPort && startPort < 65536),
    "startPort should be between 1024 and 65535 (inclusive), " + "or 0 for a random free port.")

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    val maxRetries = 100
    for (offset <- 0 to maxRetries) {
      val tryPort = if (startPort == 0) {
        startPort
      } else {
        ((startPort + offset - 1024) % (65536 - 1024)) + 1024
      }
      try {
        val (service, port) = startService(tryPort)
        System.out.println(s"Successfully started service$serviceString on port $port.")
        return (service, port)
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage = (
              s"${e.getMessage}: Service$serviceString failed after " +
              s"$maxRetries retries!"
            )
            val exception = new BindException(exceptionMessage)
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          System.out.println(s"Service$serviceString could not bind on ort $tryPort. " +
          s"Attempting port ${tryPort + 1}.")
      }
    }
    throw new SparkException(
      s"Failed to start service$serviceString on port $startPort"
    )
  }

  def isBindCollision(exception: Throwable): Boolean = {

    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

  private[example] def fakeClassTag[T]: ClassTag[T] =
    ClassTag.AnyRef.asInstanceOf[ClassTag[T]]
}
