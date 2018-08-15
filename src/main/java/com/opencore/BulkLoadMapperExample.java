package com.opencore;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This is an example mapper to be used for HBase bulk loads.
 *
 * It works with the typical input provided by the {@link org.apache.hadoop.mapreduce.lib.input.TextInputFormat}.
 *
 * It just takes its input and stores it in HBase as key -> value in a Column Family "cf" and a column "data".
 */
public class BulkLoadMapperExample extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

  private static final byte[] CF_BYTES = Bytes.toBytes("cf");
  private static final byte[] QUAL_BYTES = Bytes.toBytes("data");

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    if (value.getLength() == 0) {
      return;
    }

    byte[] rowKey = Bytes.toBytes(key.get());

    Put put = new Put(rowKey);
    put.addColumn(CF_BYTES, QUAL_BYTES, value.copyBytes());

    context.write(new ImmutableBytesWritable(rowKey), put);
  }

}
