package edu.neu.ccs.sectortopskills;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.objects.UserProfile;

public class PlainJobRunner {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 4) {

			System.err.println("Usage: 	tagindustry <in> <out> <year> <city_countryfile>");
			System.exit(2);
		}

		Job job = new Job(conf, "Top Skills Per Sector");

		job.setJarByClass(PlainJobRunner.class);
		job.setMapperClass(PlainJobMapper.class);
		job.setReducerClass(PlainJobReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(UserProfile.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.getConfiguration().set("year", otherArgs[2]);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		FileSystem hdfs = FileSystem.get(job.getConfiguration());
		FileSystem s3fs = FileSystem.get(URI.create(otherArgs[3]),job.getConfiguration());
		
		s3fs.copyToLocalFile(new Path(URI.create(otherArgs[3])), new Path(Constants.COUNTRY_CITY_CSV + "_"));
		hdfs.copyFromLocalFile(new Path(Constants.COUNTRY_CITY_CSV + "_"), new Path(Constants.COUNTRY_CITY_CSV));
		new File(Constants.COUNTRY_CITY_CSV + "_").delete();

		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}
}
