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
package com.lancedb.lance;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lancedb.lance.WriteParams.WriteMode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DatasetTest {

  @TempDir static Path tempDir; // Temporary directory for the tests
  private static Dataset dataset;

  @BeforeAll
  static void setup() {}

  @AfterAll
  static void tearDown() {
    // Cleanup resources used by the tests
    if (dataset != null) {
      dataset.close();
    }
  }

  @Test
  void testWriteStreamAndOpenPath() throws URISyntaxException, IOException {
    Path path = Paths.get(DatasetTest.class.getResource("/random_access.arrow").toURI());
    try (BufferAllocator allocator = new RootAllocator();
        ArrowFileReader reader =
            new ArrowFileReader(
                new SeekableReadChannel(
                    new ByteArrayReadableSeekableByteChannel(Files.readAllBytes(path))),
                allocator);
        ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {
      Data.exportArrayStream(allocator, reader, arrowStream);
      Path datasetPath = tempDir.resolve("new_dataset");
      assertDoesNotThrow(
          () -> {
            dataset =
                Dataset.write(
                    arrowStream,
                    datasetPath.toString(),
                    new WriteParams.Builder()
                        .withMaxRowsPerFile(10)
                        .withMaxRowsPerGroup(20)
                        .withMode(WriteMode.CREATE)
                        .build());
            assertEquals(9, dataset.countRows());
            Dataset datasetRead = Dataset.open(datasetPath.toString(), allocator);
            assertEquals(9, datasetRead.countRows());
          });

      var fragments = dataset.getFragments();
      assertEquals(1, fragments.size());
      assertEquals(0, fragments.get(0).getFragmentId());
      assertEquals(9, fragments.get(0).countRows());
    }
  }

  @Test
  void testWriteSchemaOnly() throws URISyntaxException, IOException {
    Path path = Paths.get(DatasetTest.class.getResource("/schema_only.arrow").toURI());
    try (BufferAllocator allocator = new RootAllocator();
        ArrowFileReader reader =
            new ArrowFileReader(
                new SeekableReadChannel(
                    new ByteArrayReadableSeekableByteChannel(Files.readAllBytes(path))),
                allocator);
        ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {
      Data.exportArrayStream(allocator, reader, arrowStream);
      Path datasetPath = tempDir.resolve("new_dataset");
      assertDoesNotThrow(
          () -> {
            dataset =
                Dataset.write(
                    arrowStream,
                    datasetPath.toString(),
                    new WriteParams.Builder()
                        .withMaxRowsPerFile(10)
                        .withMaxRowsPerGroup(20)
                        .withMode(WriteMode.CREATE)
                        .build());
            assertEquals(0, dataset.countRows());
            Dataset datasetRead = Dataset.open(datasetPath.toString(), allocator);
            assertEquals(0, datasetRead.countRows());
          });

      var fragments = dataset.getFragments();
      assertEquals(0, fragments.size());
    }
  }

  @Test
  void testWriteSchemaOnlyFile() throws URISyntaxException, IOException {
    Path path = Paths.get(DatasetTest.class.getResource("/random_access.arrow").toURI());
    try (BufferAllocator allocator = new RootAllocator();
         ArrowFileReader reader =
             new ArrowFileReader(
                 new SeekableReadChannel(
                     new ByteArrayReadableSeekableByteChannel(Files.readAllBytes(path))),
                 allocator);
         ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {
      Schema schema = reader.getVectorSchemaRoot().getSchema();
      Data.exportArrayStream(allocator, reader, arrowStream);
      Path datasetPath = tempDir.resolve("new_dataset");
      assertDoesNotThrow(
          () -> {
            dataset =
                Dataset.write(
                    arrowStream,
                    datasetPath.toString(),
                    new WriteParams.Builder()
                        .withMaxRowsPerFile(10)
                        .withMaxRowsPerGroup(20)
                        .withMode(WriteMode.CREATE)
                        .build());
            assertEquals(0, dataset.countRows());
            Dataset datasetRead = Dataset.open(datasetPath.toString(), allocator);
            assertEquals(0, datasetRead.countRows());
          });

      var fragments = dataset.getFragments();
      assertEquals(0, fragments.size());
    }
  }

  @Test
  void testWrite() throws URISyntaxException, IOException {
    Schema schema = new Schema(Arrays.asList(
        new Field("id", new FieldType(false, new ArrowType.Int(32, true), null), null),
        new Field("data", new FieldType(false, new ArrowType.Utf8(), null), null)
        ));

    try (RootAllocator allocator = new RootAllocator();
         IntVector idVector = new IntVector("id", allocator);
         VarCharVector dataVector = new VarCharVector("data", allocator)) {
      // Allocate vector resources
      idVector.allocateNew();
      dataVector.allocateNew();

      // Set values
      idVector.setSafe(0, 1); // ID = 1
      dataVector.setSafe(0, "Hello Arrow".getBytes(StandardCharsets.UTF_8)); // data = "Hello Arrow"
      idVector.setValueCount(1);
      dataVector.setValueCount(1);

      // Create a VectorSchemaRoot pointing to the vectors
      VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(idVector.getField(), dataVector.getField()), Arrays.asList(idVector, dataVector));
      root.setRowCount(1);

      // Write to a file
      try (FileOutputStream out = new FileOutputStream("/Users/alluxio/alluxioFolder/lance/java/core/src/test/resources/simple_arrow_file.arrow")) {
        try (ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out))) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }
      }
    }
  }

  @Test
  void testWriteStreamSchemaOnly() throws URISyntaxException, IOException {
    Schema schema = new Schema(Arrays.asList(
        new Field("id", new FieldType(false, new ArrowType.Int(32, true), null), null),
        new Field("data", new FieldType(false, new ArrowType.Utf8(), null), null)
    ));

    createEmptyDataset(schema);
  }

  @Test
  void testWriteStreamCreateAppend() throws URISyntaxException, IOException {
    Path path = Paths.get(DatasetTest.class.getResource("/random_access.arrow").toURI());
    Schema schema;
    try (BufferAllocator allocator = new RootAllocator();
         ArrowFileReader reader = new ArrowFileReader(
             new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(Files.readAllBytes(path))),
             allocator);
         ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {

      // Extract the schema from the ArrowFileReader
      schema = reader.getVectorSchemaRoot().getSchema();
    }
    createEmptyDataset(schema);
    try (BufferAllocator allocator = new RootAllocator();
         ArrowFileReader reader =
             new ArrowFileReader(
                 new SeekableReadChannel(
                     new ByteArrayReadableSeekableByteChannel(Files.readAllBytes(path))),
                 allocator);
         ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {
      Data.exportArrayStream(allocator, reader, arrowStream);
      Path datasetPath = tempDir.resolve("new_dataset");
      assertDoesNotThrow(
          () -> {
            dataset =
                Dataset.write(
                    arrowStream,
                    datasetPath.toString(),
                    new WriteParams.Builder()
                        .withMaxRowsPerFile(10)
                        .withMaxRowsPerGroup(20)
                        .withMode(WriteMode.CREATE)
                        .build());
            assertEquals(9, dataset.countRows());
            Dataset datasetRead = Dataset.open(datasetPath.toString(), allocator);
            assertEquals(9, datasetRead.countRows());
          });

      var fragments = dataset.getFragments();
      assertEquals(1, fragments.size());
      assertEquals(0, fragments.get(0).getFragmentId());
      assertEquals(9, fragments.get(0).countRows());
    }
  }
  
  private void createEmptyDataset(Schema schema) throws IOException {
    try (RootAllocator allocator = new RootAllocator();
         VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      ByteArrayOutputStream schemaOnlyOutStream = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(schemaOnlyOutStream))) {
        writer.start();
        writer.end();
      }
      ByteArrayInputStream schemaOnlyInStream = new ByteArrayInputStream(schemaOnlyOutStream.toByteArray());
      try (ArrowStreamReader reader = new ArrowStreamReader(schemaOnlyInStream, allocator);
           ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {
        Data.exportArrayStream(allocator, reader, arrowStream);
        Path datasetPath = tempDir.resolve("new_dataset");
        assertDoesNotThrow(
            () -> {
              dataset =
                  Dataset.write(
                      arrowStream,
                      datasetPath.toString(),
                      new WriteParams.Builder()
                          .withMaxRowsPerFile(10)
                          .withMaxRowsPerGroup(20)
                          .withMode(WriteMode.CREATE)
                          .build());
              assertEquals(0, dataset.countRows());
              Dataset datasetRead = Dataset.open(datasetPath.toString(), allocator);
              assertEquals(0, datasetRead.countRows());
            });

        var fragments = dataset.getFragments();
        assertEquals(0, fragments.size());
      }
    }
  }

  @Test
  void testOpenInvalidPath() {
    String validPath = tempDir.resolve("Invalid_dataset").toString();
    assertThrows(
        RuntimeException.class,
        () -> {
          dataset = Dataset.open(validPath, new RootAllocator());
        });
  }
}
