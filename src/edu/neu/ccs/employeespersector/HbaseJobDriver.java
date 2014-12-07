package edu.neu.ccs.employeespersector;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.objects.UserProfile;

public class HbaseJobDriver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 4) {

			System.err.println("Usage: emp_per_sector_per_region_hbase <in_hbase_table> <out> <year> <city_countryfile>");
			System.exit(4);
		}
		
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem s3fs = FileSystem.get(URI.create(otherArgs[3]),conf);
		
		s3fs.copyToLocalFile(new Path(URI.create(otherArgs[3])), new Path(Constants.COUNTRY_CITY_CSV + "_"));
		hdfs.copyFromLocalFile(new Path(Constants.COUNTRY_CITY_CSV + "_"), new Path(Constants.COUNTRY_CITY_CSV));
		new File(Constants.COUNTRY_CITY_CSV + "_").delete();

		Job job = new Job(conf, "Employees Per Sector Per Region - Hbase");

		job.setJarByClass(HbaseJobDriver.class);
		job.setMapperClass(HbaseJobMapper.class);
		job.setReducerClass(PlainJobReducer.class);
		
		Scan scan = new Scan();
        scan.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false); // don't set to true for MR jobs set other scan attrs
        
        //Start scanning
        scan.setStartRow(Bytes.toBytes(otherArgs[2] + "*"));
        
        //Stop Scanning
        scan.setStopRow(Bytes.toBytes((Integer.parseInt(otherArgs[2]) + 1) + "*"));
        
        TableMapReduceUtil.initTableMapperJob(
                Bytes.toBytes(otherArgs[0]), // input table
                scan, // Scan instance to control CF and attribute selection
                HbaseJobMapper.class, // mapper class
                Text.class, // mapper output key
                UserProfile.class, // mapper output value
                job);
        
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(UserProfile.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//System.exit(job.waitForCompletion(true) ? 1 : 0);
		job.waitForCompletion(true);
	}

}
