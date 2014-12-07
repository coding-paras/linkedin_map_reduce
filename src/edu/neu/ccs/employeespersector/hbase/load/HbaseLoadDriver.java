package edu.neu.ccs.employeespersector.hbase.load;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.neu.ccs.constants.Constants;

public class HbaseLoadDriver {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: loadlinkedindata <table_name> <input_files>");
			System.exit(2);
		}
		
		createHbaseTable(otherArgs[0]);

		conf.set(TableOutputFormat.OUTPUT_TABLE, otherArgs[0]);
		
		Job job = new Job(conf, "Load Linkedin Data - HBase");
		job.setJarByClass(HbaseLoadDriver.class);
		job.setMapperClass(HbaseLoadMapper.class);

		job.setOutputFormatClass(TableOutputFormat.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        job.setNumReduceTasks(0); //Setting number of reduce tasks to 0

		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	private static void createHbaseTable(String tableName) throws IOException {
		
		@SuppressWarnings("deprecation")
		HBaseConfiguration hc = new HBaseConfiguration(new Configuration());

		HTableDescriptor ht = new HTableDescriptor(tableName);

		ht.addFamily(new HColumnDescriptor(Constants.COLUMN_FAMILY));
		System.out.println("connecting..");

		HBaseAdmin hba = new HBaseAdmin(hc);

		System.out.println("creating table..");

		hba.createTable(ht);

		System.out.println("table created.");
		
		hba.close();
	}

}
