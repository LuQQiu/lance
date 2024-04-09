/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lancedb.lance.spark.source;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

/**
 * Spark write builder.
 */
public class SparkWriteBuilder implements WriteBuilder {
  private final String datasetUri;
  private final Schema arrowSchema;
  private final LogicalWriteInfo info;
  
  SparkWriteBuilder(String datasetUri, Schema arrowSchema, LogicalWriteInfo info) {
    this.datasetUri = datasetUri;
    this.arrowSchema = arrowSchema;
    this.info = info;
  }

  @Override
  public Write build() {
    return new SparkWrite(datasetUri, arrowSchema, info);
  }
}
