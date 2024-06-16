package com.lancedb.lance.spark;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.platform.commons.util.Preconditions;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class ArrowWriterReaderTest {
  public static class CustomArrowReader extends ArrowReader {
    private final Schema schema;
    private final Object monitor = new Object();
    @GuardedBy("monitor")
    private final Queue<ArrowRecordBatch> batchQueue = new ArrayDeque<>();
    @GuardedBy("monitor")
    private boolean finished; 
    
    private final AtomicLong totalBytesRead = new AtomicLong();
    

    public CustomArrowReader(BufferAllocator allocator, Schema schema) {
      super(allocator);
      this.schema = schema;
    }

    public void addBatch(ArrowRecordBatch batch) {
      Preconditions.notNull(batch, "batch not null");
      synchronized (monitor) {
        batchQueue.offer(batch);
        monitor.notify();
      }
    }

    public void markNoMoreBatches() {
      synchronized (monitor) {
        finished = true;
        monitor.notifyAll();
      }
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      ArrowRecordBatch batch = null;
      synchronized (monitor) {
        while (batchQueue.isEmpty() && !finished) {
          try {
            wait();  // Wait for new batches to be added or for no more batches
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for the next batch", e);
          }
        }
        if (!batchQueue.isEmpty()) {
          batch = batchQueue.poll();
        }
      }
      if (batch == null) {
        return false;
      }
      prepareLoadNextBatch();
      loadRecordBatch(batch); // batch close inside the loadRecordBatch
      totalBytesRead.addAndGet(batch.computeBodyLength());
      return true;
    }

    @Override
    public long bytesRead() {
      return totalBytesRead.get();
    }

    @Override
    protected synchronized void closeReadSource() throws IOException {
      // Implement if needed
    }

    @Override
    protected Schema readSchema() {
      return this.schema;
    }
  }

  public static class IncrementalArrowWriter {
    private BufferAllocator allocator;
    private Schema schema;
    private VectorSchemaRoot root;
    private CustomArrowReader customArrowReader;

    public IncrementalArrowWriter(Schema schema) {
      this.allocator = new RootAllocator(Long.MAX_VALUE);
      this.schema = schema;
      this.root = VectorSchemaRoot.create(schema, allocator);
      this.customArrowReader = new CustomArrowReader(allocator, schema);
    }

    public void writeBatch(List<InternalRow> rows) {
      try {
        writeInternalRowsToRoot(rows);
        VectorUnloader unloader = new VectorUnloader(root);
        ArrowRecordBatch batch = unloader.getRecordBatch();
        customArrowReader.addBatch(batch);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    private void writeInternalRowsToRoot(List<InternalRow> rows) {
      root.setRowCount(0);
      IntVector idVector = (IntVector) root.getVector("id");
      VarCharVector nameVector = (VarCharVector) root.getVector("name");

      idVector.allocateNew(rows.size());
      nameVector.allocateNew(rows.size());

      for (int i = 0; i < rows.size(); i++) {
        InternalRow row = rows.get(i);
        idVector.setSafe(i, row.getInt(0));
        UTF8String utf8Str = row.getUTF8String(1);
        nameVector.setSafe(i, utf8Str.getBytes());
      }

      idVector.setValueCount(rows.size());
      nameVector.setValueCount(rows.size());
      root.setRowCount(rows.size());
    }

    public void markNoMoreBatches() {
      customArrowReader.markNoMoreBatches();
    }

    public CustomArrowReader getCustomArrowReader() {
      return customArrowReader;
    }

    public void close() {
      try {
        customArrowReader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      root.close();
      allocator.close();
    }
  }

  public static void main(String[] args) {
    // Initialize Arrow environment
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    // Define schema with FieldType
    Schema schema = new Schema(Arrays.asList(
        new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
    ));

    // Create InternalRow data for four batches
    List<InternalRow> rows1 = new ArrayList<>();
    rows1.add(new GenericInternalRow(new Object[]{1, UTF8String.fromString("Alice")}));
    rows1.add(new GenericInternalRow(new Object[]{2, UTF8String.fromString("Bob")}));

    List<InternalRow> rows2 = new ArrayList<>();
    rows2.add(new GenericInternalRow(new Object[]{3, UTF8String.fromString("Carol")}));
    rows2.add(new GenericInternalRow(new Object[]{4, UTF8String.fromString("Dave")}));

    List<InternalRow> rows3 = new ArrayList<>();
    rows3.add(new GenericInternalRow(new Object[]{5, UTF8String.fromString("Eve")}));
    rows3.add(new GenericInternalRow(new Object[]{6, UTF8String.fromString("Frank")}));

    List<InternalRow> rows4 = new ArrayList<>();
    rows4.add(new GenericInternalRow(new Object[]{7, UTF8String.fromString("Grace")}));
    rows4.add(new GenericInternalRow(new Object[]{8, UTF8String.fromString("Heidi")}));

    // Initialize writer
    IncrementalArrowWriter writer = new IncrementalArrowWriter(schema);
    CustomArrowReader reader = writer.getCustomArrowReader();

    // Interleaved writing and reading
    Thread writerThread = new Thread(() -> {
      writer.writeBatch(rows1);
      writer.writeBatch(rows2);
      writer.writeBatch(rows3);
      writer.writeBatch(rows4);
      writer.markNoMoreBatches();
    });

    Thread readerThread = new Thread(() -> {
      List<List<InternalRow>> allBatches = Arrays.asList(rows1, rows2, rows3, rows4);
      int batchIndex = 0;

      while (true) {
        try {
          if (!reader.loadNextBatch()) {
            break;
          }
          validateData(reader, allBatches.get(batchIndex));
          batchIndex++;
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });

    writerThread.start();
    readerThread.start();

    try {
      writerThread.join();
      readerThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Close resources
    writer.close();
  }

  private static void validateData(CustomArrowReader reader, List<InternalRow> expectedRows) {
    try {
      if (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        for (int i = 0; i < root.getRowCount(); i++) {
          InternalRow expectedRow = expectedRows.get(i);
          // Validate each field
          int id = (int) root.getVector("id").getObject(i);
          String name = root.getVector("name").getObject(i).toString();
          assert id == (int) expectedRow.get(0, DataTypes.IntegerType);
          assert name.equals(expectedRow.getString(1));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

// Think about the two appraoch first
// TODO(lu) ensure resource cleanup!!!!!!