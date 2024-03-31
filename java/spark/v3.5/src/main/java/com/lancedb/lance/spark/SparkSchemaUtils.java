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

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Spark schema utils.
 */
public class SparkSchemaUtils {
  /**
   * Convert Spark struct type to Arrow schema.
   *
   * @param structType spark struct type
   * @return Arrow schema
   */
  public static Schema convert(StructType structType) {
    List<Field> arrowFields = new ArrayList<>();
    for (StructField field : structType.fields()) {
      arrowFields.add(new Field(field.name(), 
          new FieldType(field.nullable(), convert(field.dataType()), null, null), 
          null));
    }
    return new Schema(arrowFields);
  }

  private static ArrowType convert(DataType dataType) {
    if (dataType instanceof IntegerType) {
      return new ArrowType.Int(32, true);
    } else if (dataType instanceof LongType) {
      return new ArrowType.Int(64, true);
    } else if (dataType instanceof StringType) {
      return new ArrowType.Utf8();
    } else if (dataType instanceof DoubleType) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    } else if (dataType instanceof FloatType) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    } else {
      throw new UnsupportedOperationException("Unsupported Spark type: " + dataType);
    }
  }

  private SparkSchemaUtils() {}
}
