/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.streaming.sources

import java.io._
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import org.apache.commons.io.IOUtils

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReadSupport, Offset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{ManualClock, SystemClock}

class RateStreamMicroBatchReadSupport(options: DataSourceOptions, checkpointLocation: String)
  extends MicroBatchReadSupport with Logging {
  import RateStreamProvider._

  private[sources] val clock = {
    // The option to use a manual clock is provided only for unit testing purposes.
    if (options.getBoolean("useManualClock", false)) new ManualClock else new SystemClock
  }

  private val rowsPerSecond =
    options.get(ROWS_PER_SECOND).orElse("1").toLong

  private val rampUpTimeSeconds =
    Option(options.get(RAMP_UP_TIME).orElse(null.asInstanceOf[String]))
      .map(JavaUtils.timeStringAsSec(_))
      .getOrElse(0L)

  private val maxSeconds = Long.MaxValue / rowsPerSecond

  if (rampUpTimeSeconds > maxSeconds) {
    throw new ArithmeticException(
      s"Integer overflow. Max offset with $rowsPerSecond rowsPerSecond" +
        s" is $maxSeconds, but 'rampUpTimeSeconds' is $rampUpTimeSeconds.")
  }

  private[sources] val creationTimeMs = {
    val session = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    require(session.isDefined)

    val metadataLog =
      new HDFSMetadataLog[LongOffset](session.get, checkpointLocation) {
        override def serialize(metadata: LongOffset, out: OutputStream): Unit = {
          val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
          writer.write("v" + VERSION + "\n")
          writer.write(metadata.json)
          writer.flush
        }

        override def deserialize(in: InputStream): LongOffset = {
          val content = IOUtils.toString(new InputStreamReader(in, StandardCharsets.UTF_8))
          // HDFSMetadataLog guarantees that it never creates a partial file.
          assert(content.length != 0)
          if (content(0) == 'v') {
            val indexOfNewLine = content.indexOf("\n")
            if (indexOfNewLine > 0) {
              parseVersion(content.substring(0, indexOfNewLine), VERSION)
              LongOffset(SerializedOffset(content.substring(indexOfNewLine + 1)))
            } else {
              throw new IllegalStateException(
                s"Log file was malformed: failed to detect the log file version line.")
            }
          } else {
            throw new IllegalStateException(
              s"Log file was malformed: failed to detect the log file version line.")
          }
        }
      }

    metadataLog.get(0).getOrElse {
      val offset = LongOffset(clock.getTimeMillis())
      metadataLog.add(0, offset)
      logInfo(s"Start time: $offset")
      offset
    }.offset
  }

  @volatile private var lastTimeMs: Long = creationTimeMs

  override def initialOffset(): Offset = LongOffset(0L)

  override def latestOffset(): Offset = {
    val now = clock.getTimeMillis()
    if (lastTimeMs < now) {
      lastTimeMs = now
    }
    LongOffset(TimeUnit.MILLISECONDS.toSeconds(lastTimeMs - creationTimeMs))
  }

  override def deserializeOffset(json: String): Offset = {
    LongOffset(json.toLong)
  }

  override def fullSchema(): StructType = SCHEMA

  override def newScanConfigBuilder(start: Offset, end: Offset): ScanConfigBuilder = {
    new SimpleStreamingScanConfigBuilder(fullSchema(), start, Some(end))
  }

  override def planInputPartitions(config: ScanConfig): Array[InputPartition] = {
    val sc = config.asInstanceOf[SimpleStreamingScanConfig]
    val startSeconds = sc.start.asInstanceOf[LongOffset].offset
    val endSeconds = sc.end.get.asInstanceOf[LongOffset].offset
    assert(startSeconds <= endSeconds, s"startSeconds($startSeconds) > endSeconds($endSeconds)")
    if (endSeconds > maxSeconds) {
      throw new ArithmeticException("Integer overflow. Max offset with " +
        s"$rowsPerSecond rowsPerSecond is $maxSeconds, but it's $endSeconds now.")
    }
    // Fix "lastTimeMs" for recovery
    if (lastTimeMs < TimeUnit.SECONDS.toMillis(endSeconds) + creationTimeMs) {
      lastTimeMs = TimeUnit.SECONDS.toMillis(endSeconds) + creationTimeMs
    }
    val rangeStart = valueAtSecond(startSeconds, rowsPerSecond, rampUpTimeSeconds)
    val rangeEnd = valueAtSecond(endSeconds, rowsPerSecond, rampUpTimeSeconds)
    logDebug(s"startSeconds: $startSeconds, endSeconds: $endSeconds, " +
      s"rangeStart: $rangeStart, rangeEnd: $rangeEnd")

    if (rangeStart == rangeEnd) {
      return Array.empty
    }

    val localStartTimeMs = creationTimeMs + TimeUnit.SECONDS.toMillis(startSeconds)
    val relativeMsPerValue =
      TimeUnit.SECONDS.toMillis(endSeconds - startSeconds).toDouble / (rangeEnd - rangeStart)
    val numPartitions = {
      val activeSession = SparkSession.getActiveSession
      require(activeSession.isDefined)
      Option(options.get(NUM_PARTITIONS).orElse(null.asInstanceOf[String]))
        .map(_.toInt)
        .getOrElse(activeSession.get.sparkContext.defaultParallelism)
    }

    (0 until numPartitions).map { p =>
      new RateStreamMicroBatchInputPartition(
        p, numPartitions, rangeStart, rangeEnd, localStartTimeMs, relativeMsPerValue)
    }.toArray
  }

  override def createReaderFactory(config: ScanConfig): PartitionReaderFactory = {
    RateStreamMicroBatchReaderFactory
  }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {}

  override def toString: String = s"RateStreamV2[rowsPerSecond=$rowsPerSecond, " +
    s"rampUpTimeSeconds=$rampUpTimeSeconds, " +
    s"numPartitions=${options.get(NUM_PARTITIONS).orElse("default")}"
}

case class RateStreamMicroBatchInputPartition(
    partitionId: Int,
    numPartitions: Int,
    rangeStart: Long,
    rangeEnd: Long,
    localStartTimeMs: Long,
    relativeMsPerValue: Double) extends InputPartition

object RateStreamMicroBatchReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[RateStreamMicroBatchInputPartition]
    new RateStreamMicroBatchPartitionReader(p.partitionId, p.numPartitions, p.rangeStart,
      p.rangeEnd, p.localStartTimeMs, p.relativeMsPerValue)
  }
}

class RateStreamMicroBatchPartitionReader(
    partitionId: Int,
    numPartitions: Int,
    rangeStart: Long,
    rangeEnd: Long,
    localStartTimeMs: Long,
    relativeMsPerValue: Double) extends PartitionReader[InternalRow] {
  private var count: Long = 0

  override def next(): Boolean = {
    rangeStart + partitionId + numPartitions * count < rangeEnd
  }

  override def get(): InternalRow = {
    val currValue = rangeStart + partitionId + numPartitions * count
    count += 1
    val relative = math.round((currValue - rangeStart) * relativeMsPerValue)
    InternalRow(DateTimeUtils.fromMillis(relative + localStartTimeMs), currValue)
  }

  override def close(): Unit = {}
}
