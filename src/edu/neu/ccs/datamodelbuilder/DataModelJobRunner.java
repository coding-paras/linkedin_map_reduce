package edu.neu.ccs.datamodelbuilder;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.neu.ccs.constants.Constants;

public class DataModelJobRunner {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();

		if (otherArgs.length != 4) {
			System.err.println("Usage: datamodelbuilder <in> <out> <cache> <output_job1>");
			System.exit(4);
		}

		Job job = new Job(conf, "Data Model Builder");

		job.setJarByClass(DataModelJobRunner.class);
		job.setMapperClass(DataModelMapper.class);
		job.setReducerClass(DataModelReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Setting distributed cache of industry to sector mapping.
		DistributedCache.addCacheFile(new URI(otherArgs[2]), job.getConfiguration());
		
		conf.set(Constants.INDUSTRY_SECTOR_FILE, otherArgs[2]); //DistributedCache filename
		
		//Distributed Cache - HDFS Output from Job1
		
		
		//Displaying the counters and their values
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
