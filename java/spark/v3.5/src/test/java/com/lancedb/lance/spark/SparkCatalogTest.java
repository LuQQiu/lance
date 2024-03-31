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

package com.lancedb.lance.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;

public class SparkCatalogTest {

  @TempDir
  static Path tempDir;

  private static SparkSession spark;

  @BeforeAll
  static void setup() {
    spark = SparkSession.builder()
        .appName("SparkCatalogTest")
        .master("local")
        .config("spark.sql.catalog.dev", "com.lancedb.lance.spark.SparkCatalog")
        .config("spark.sql.catalog.dev.type", "hadoop")
        .config("spark.sql.catalog.dev.warehouse", tempDir.toString())
        .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testSparkCreate() {
    String tableName = "dev.db.lance_table";
    spark.sql("CREATE TABLE " + tableName + " (id INT, data STRING) USING lance");
  }
}
