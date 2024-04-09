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

import com.lancedb.lance.Fragment;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.lancedb.lance.WriteParams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.execution.arrow.ArrowWriter;

/**
 * Spark write.
 */
public class SparkWrite implements Write {
  private final String datasetUri;
  private final Schema arrowSchema;
  private final LogicalWriteInfo info;
  
  SparkWrite(String datasetUri, Schema arrowSchema, LogicalWriteInfo info) {
    this.datasetUri = datasetUri;
    this.arrowSchema = arrowSchema;
    this.info = info;
  }

  @Override
  public BatchWrite toBatch() {
    return new BatchAppend();
  }

  @Override
  public StreamingWrite toStreaming() {
    throw new UnsupportedOperationException();
  }

  private WriterFactory createWriterFactory() {
    return new WriterFactory(datasetUri, arrowSchema);
  }

  private class BatchAppend extends BaseBatchWrite {
    @Override
    public void commit(WriterCommitMessage[] messages) {
      // TODO(lu) LanceOperation.Append
      // TODO(lu) commit
    }
  }

  private abstract class BaseBatchWrite implements BatchWrite {
    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      return createWriterFactory();
    }

    @Override
    public boolean useCommitCoordinator() {
      return false;
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return String.format("LanceBatchWrite(datasetUri=%s)", datasetUri);
    }
  }

  private static class WriterFactory implements DataWriterFactory {
    private final String datasetUri;
    private final Schema arrowSchema;

    protected WriterFactory(String datasetUri, Schema arrowSchema) {
      // Execute at Spark executor
      this.datasetUri = datasetUri;
      this.arrowSchema = arrowSchema;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      return new UnpartitionedDataWriter(datasetUri, arrowSchema);
    }
  }

  private static class UnpartitionedDataWriter implements DataWriter<InternalRow> {
    private final String datasetUri;
    private final BufferAllocator allocator;
    private final VectorSchemaRoot root;
    private final ArrowWriter writer;
    
    private UnpartitionedDataWriter(String datasetUri, Schema arrowSchema) {
      // TODO(lu) maxRecordPerBatch, turn to batch write
      this.datasetUri = datasetUri;
      this.allocator = new RootAllocator(Long.MAX_VALUE);
      root = VectorSchemaRoot.create(arrowSchema, allocator);
      writer = ArrowWriter.create(root);
    }

    @Override
    public void write(InternalRow record) {
      writer.write(record);
    }

    @Override
    public WriterCommitMessage commit() {
      writer.finish();
      return new TaskCommit(Arrays.asList(
          Fragment.create(datasetUri, allocator, root,
              Optional.empty(), new WriteParams.Builder().build())
              .getFragementId()));
    }

    @Override
    public void abort() {
      close();
    }

    @Override
    public void close() {
      writer.reset();
      root.close();
      allocator.close();
    }
  }

  /** Task commit. */
  public static class TaskCommit implements WriterCommitMessage {
    private final List<Integer> fragments;

    TaskCommit(List<Integer> fragments) {
      this.fragments = fragments;
    }

    List<Integer> getFragments() {
      return fragments;
    }
  }
}
