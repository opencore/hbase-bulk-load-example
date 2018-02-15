package com.opencore;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class BulkLoadMapper extends Mapper<NullWritable, NullWritable, ImmutableBytesWritable, Put> {

  private static final byte[] CF_BYTES = Bytes.toBytes("cf");
  private static final byte[] QUAL_BYTES = Bytes.toBytes("q");

  // TODO: NullWritable needs to be replaced by the correct one, depends on the InputFormat
  @Override
  protected void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
    // TODO: Build a Put object from your key and value data here

    byte[] rowKey = "demo row key".getBytes();
    byte[] valueBytes = "demo value".getBytes();

    Put put = new Put(rowKey);


    // TODO: Call addColumn for each column to add. You'd obviously use multiple different qualifiers
    put.addColumn(CF_BYTES, QUAL_BYTES, valueBytes);
    // ...

    context.write(new ImmutableBytesWritable(rowKey), put);
  }

}
