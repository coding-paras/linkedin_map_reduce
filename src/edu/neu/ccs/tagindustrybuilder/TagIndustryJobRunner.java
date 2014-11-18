package edu.neu.ccs.tagindustrybuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Job 1 that emits (tag,[industry1, industry2,....]) pair representing the
 * industries that relates to a skill. Also, it emits (year, numberOfRecords)
 * pair representing number of records for each year in the dataset.
 */
public class TagIndustryJobRunner {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: tagindustry <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Tag Industry");
		
		job.setJarByClass(TagIndustryJobRunner.class);
		job.setMapperClass(TagIndustryMapper.class);
		job.setReducerClass(TagIndustryReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		// Setting number of reduce tasks to 10
		job.setNumReduceTasks(10);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
