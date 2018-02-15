package com.opencore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BulkLoadDriver extends Configured implements Tool {

  public static void main(String... args) throws Exception {
    int status = ToolRunner.run(new BulkLoadDriver(), args);
    System.exit(status);
  }

  private static Job createSubmittableJob(Configuration conf, String tableNameString, Path tmpPath)
    throws IOException {
    conf.setBoolean("mapreduce.map.speculative", false);
    conf.setBoolean("mapreduce.reduce.speculative", false);

    Job job = Job.getInstance(conf, "HBase Bulk Load Example");

    job.setJarByClass(BulkLoadMapper.class);
    job.setMapperClass(BulkLoadMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);

    FileOutputFormat.setOutputPath(job, tmpPath);

    // TODO: Configure input format here
    //job.setInputFormatClass(TODO);

    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      TableName tableName = TableName.valueOf(tableNameString);
      Table table = connection.getTable(tableName);
      RegionLocator regionLocator = connection.getRegionLocator(tableName);
      HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
    }
    return job;
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create(getConf());
    setConf(conf);

    List<String> otherArgs = new ArrayList<>(Arrays.asList(new GenericOptionsParser(getConf(), args).getRemainingArgs()));
    if (otherArgs.size() < 2) {
      System.err.println("Wrong number of arguments: " + otherArgs.size());
      System.err.println("Need two arguments '-table' and '-tmpPath'");
      return -1;
    }

    String tableNameString = StringUtils.popOptionWithArgument("-table", otherArgs);
    Path tmpPath = new Path(StringUtils.popOptionWithArgument("-tmpPath", otherArgs));

    Job job = createSubmittableJob(conf, tableNameString, tmpPath);
    boolean success = job.waitForCompletion(true);

    doBulkLoad(tableNameString);

    return success ? 0 : 1;
  }

  private void doBulkLoad(String tableNameString) throws Exception {
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConf());
    try (Connection connection = ConnectionFactory.createConnection(getConf()); Admin admin = connection.getAdmin()) {
      TableName tableName = TableName.valueOf(tableNameString);
      Table table = connection.getTable(tableName);
      RegionLocator regionLocator = connection.getRegionLocator(tableName);
      loader.doBulkLoad(null, admin, table, regionLocator);
    }
  }
}
