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
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.concurrent.TimeoutException;

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
  public void testCreate() {
    String tableName = "dev.db.lance_create_table";
    // Workflow: loadTable with NoSuchTable -> createTable -> loadTable
    createTable(tableName);
    // Workflow: [Gap] dropTable
    // spark.sql("DROP TABLE " + tableName);
  }

  @Test
  @Disabled
  public void testInsert() {
    String tableName = "dev.db.lance_insert_table";
    createTable(tableName);
    // Workflow: loadTable
    // -> [Gap] SparkTable.newWriteBuilder -> BatchWrite -> lance append
    spark.sql("INSERT INTO " + tableName
        + " VALUES ('100', '2015-01-01', '2015-01-01T13:51:39.340396Z'), ('101', '2015-01-01', '2015-01-01T12:14:58.597216Z')");
    // Workflow: loadTable
    // -> [Gap] SparkScanBuilder.pushAggregation().build()
    // -> [Gap] LocalScan.readSchema() -> [Gap] LocalScan.rows[]
    spark.sql("SELECT * FROM " + tableName).show();
    spark.sql("SELECT COUNT(*) FROM " + tableName).show();
  }

  @Test
  @Disabled
  public void testBatchWriteTable() throws TableAlreadyExistsException {
    Dataset<Row> data = createSparkDataFrame();
    String tableName = "dev.db.lance_df_table";
    // Same as create + insert
    data.writeTo(tableName).using("lance").create();
    spark.table(tableName).show();
    // Add check manifest created
  }

  @Test
  @Disabled
  public void testBatchAppendTable() throws NoSuchTableException {
    Dataset<Row> data = createSparkDataFrame();
    String tableName = "dev.db.lance_df_append_table";
    createTable(tableName);
    // Same as insert
    data.writeTo(tableName).append();
    spark.table(tableName).show();
  }

  @Test
  @Disabled
  public void testStreamingWriteTable() throws NoSuchTableException, TimeoutException {
    Dataset<Row> data = createSparkDataFrame();
    String tableName = "dev.db.lance_streaming_table";
    data.writeStream().format("lance").outputMode("append").toTable(tableName);
    spark.table(tableName).show();
  }

  @Test
  @Disabled
  public void testStreamingAppendTable() throws NoSuchTableException, TimeoutException {
    Dataset<Row> data = createSparkDataFrame();
    String tableName = "dev.db.lance_streaming_append_table";
    createTable(tableName);
    data.writeStream().format("lance").outputMode("append").toTable(tableName);
    spark.table(tableName).show();
  }

  private Dataset<Row> createSparkDataFrame() {
    StructType schema = new StructType(new StructField[]{
        DataTypes.createStructField("id", DataTypes.StringType, false),
        DataTypes.createStructField("creation_date", DataTypes.StringType, false),
        DataTypes.createStructField("last_update_time", DataTypes.StringType, false)
    });
    return spark.createDataFrame(java.util.Arrays.asList(
        RowFactory.create("100", "2015-01-01", "2015-01-01T13:51:39.340396Z"),
        RowFactory.create("101", "2015-01-01", "2015-01-01T12:14:58.597216Z"),
        RowFactory.create("102", "2015-01-01", "2015-01-01T13:51:40.417052Z"),
        RowFactory.create("103", "2015-01-01", "2015-01-01T13:51:40.519832Z")
    ), schema);
  }

  private void createTable(String tableName) {
    spark.sql("CREATE TABLE IF NOT EXISTS " + tableName +
        "(id STRING, " +
        "creation_date STRING, " +
        "last_update_time STRING) " +
        "USING lance");
  }
}
