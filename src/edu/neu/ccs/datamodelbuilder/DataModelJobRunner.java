package edu.neu.ccs.datamodelbuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DataModelJobRunner {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: datamodelbuilder <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "Data Model Builder");

		job.setJarByClass(DataModelJobRunner.class);
		job.setMapperClass(DataModelMapper.class);
		job.setReducerClass(DataModelReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		// Setting number of reduce tasks to 10
		job.setNumReduceTasks(10);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		//Distributed Cache - HDFS Output from Job1

		//Displaying the counters and their values
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
