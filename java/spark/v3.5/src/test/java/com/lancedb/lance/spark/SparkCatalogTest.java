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
    // spark.sql("DROP TABLE " + tableName);
  }
  
  @Test
  @Disabled
  public void testSparkEasyInsert() {
    String tableName = "dev.db.lance_table";
    spark.sql("CREATE TABLE " + tableName + " (id INT, data STRING) USING lance");
    spark.sql("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')");
    spark.sql("SELECT * FROM " + tableName).show();
    spark.sql("SELECT COUNT(*) FROM " + tableName).show();
  }

  @Test
  @Disabled
  public void testSparkDataFrame() {
    StructType schema = new StructType(new StructField[]{
        DataTypes.createStructField("id", DataTypes.StringType, false),
        DataTypes.createStructField("creation_date", DataTypes.StringType, false),
        DataTypes.createStructField("last_update_time", DataTypes.StringType, false)
    });

    // Create a DataFrame
    Dataset<Row> data = spark.createDataFrame(java.util.Arrays.asList(
        RowFactory.create("100", "2015-01-01", "2015-01-01T13:51:39.340396Z"),
        RowFactory.create("101", "2015-01-01", "2015-01-01T12:14:58.597216Z"),
        RowFactory.create("102", "2015-01-01", "2015-01-01T13:51:40.417052Z"),
        RowFactory.create("103", "2015-01-01", "2015-01-01T13:51:40.519832Z")
    ), schema);

    String tableLocation = "file://" + tempDir.resolve("db/lance_table").toString();
    spark.sql("CREATE TABLE IF NOT EXISTS dev.db.lance_table (" +
        "id STRING, " +
        "creation_date STRING, " +
        "last_update_time STRING) " +
        "USING lance " +
        "LOCATION '" + tableLocation + "'");

    data.write().format("lance").mode("append").saveAsTable("dev.db.lance_table");
    spark.table("dev.db.lance_table").show();
  }
}
