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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.sources.v2.reader.{ScanConfig, ScanConfigBuilder}
import org.apache.spark.sql.types.StructType

/**
 * A very simple [[ScanConfigBuilder]] implementation that creates a simple [[ScanConfig]] to
 * carry schema and offsets for streaming data sources.
 */
class SimpleStreamingScanConfigBuilder(
    schema: StructType,
    start: Offset,
    end: Option[Offset] = None)
  extends ScanConfigBuilder {

  override def build(): ScanConfig = SimpleStreamingScanConfig(schema, start, end)
}

case class SimpleStreamingScanConfig(
    readSchema: StructType,
    start: Offset,
    end: Option[Offset])
  extends ScanConfig
