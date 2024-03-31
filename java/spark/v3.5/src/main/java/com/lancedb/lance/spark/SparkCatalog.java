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

import com.lancedb.lance.Dataset;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import com.lancedb.lance.spark.source.SparkTable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Lance Spark Catalog.
 */
public class SparkCatalog implements TableCatalog {
  private Path warehouse = null;
  // Sequence is initialize / load table (throw no such table if not exists) / create table / loadTable again

  @Override
  public Identifier[] listTables(String[] strings) throws NoSuchNamespaceException {
    return new Identifier[0];
  }

  @Override
  public Table loadTable(Identifier identifier) throws NoSuchTableException {
    String tablePath = warehouse.resolve(identifier.name()).toString();
    try {
      Dataset.open(tablePath, new RootAllocator());
    } catch (RuntimeException | IOException e) {
      // TODO(lu) Lance::Error::DatasetNotFound => Lance Java Error
      // Other Error remains RuntimeException
      throw new NoSuchTableException(identifier);
    }
    return new SparkTable(identifier.name());
  }

  @Override
  public Table createTable(Identifier identifier, StructType structType,
      Transform[] transforms, Map<String, String> map)
      // How to create table with schema, without writing to it
      
      throws TableAlreadyExistsException, NoSuchNamespaceException {
        // identifier name lance_table namespace db
        // structType
        // fields:  StructField[]:
        // String name            id              data
        // DataType dataType     IntegerType      StringType
        // boolean nullable      true             true
        // Metadata metadata      {}              {}
        // StructType is schema
    return new SparkTable(identifier.name());
  }

  @Override
  public Table alterTable(Identifier identifier, TableChange... tableChanges)
      throws NoSuchTableException {
    return null;
  }

  @Override
  public boolean dropTable(Identifier identifier) {
    return false;
  }

  @Override
  public void renameTable(Identifier identifier, Identifier identifier1)
      throws NoSuchTableException, TableAlreadyExistsException {
  }

  @Override
  public void initialize(String s, CaseInsensitiveStringMap caseInsensitiveStringMap) {
    this.warehouse = Path.of(caseInsensitiveStringMap.get("warehouse"));
  }

  @Override
  public String name() {
    return "lance";
  }
}
