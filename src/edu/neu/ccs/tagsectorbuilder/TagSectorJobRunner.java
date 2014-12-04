package edu.neu.ccs.tagsectorbuilder;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.neu.ccs.constants.Constants;

/**
 * Job 1 that emits (tag,[industry1, industry2,....]) pair representing the
 * industries that relates to a skill. Also, it emits (year, numberOfRecords)
 * pair representing number of records for each year in the dataset.
 */
public class TagSectorJobRunner {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 3) {

			System.err.println("Usage: 	tagsector <in> <out> <cache>");
			System.exit(2);
		}

		Job job = new Job(conf, "Tag Sector");

		job.setJarByClass(TagSectorJobRunner.class);
		job.setMapperClass(TagSectorMapper.class);
		job.setReducerClass(TagSectorReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// Setting distributed cache of industry to sector mapping.
		DistributedCache.addCacheFile(new URI(otherArgs[2]),job.getConfiguration());

		job.getConfiguration().set(Constants.INDUSTRY_SECTOR_FILE, otherArgs[2]); // DistributedCache filename

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Displaying the counters and their values
		if (job.waitForCompletion(true)) {

			for (Counter counter : job.getCounters().getGroup(
					Constants.YEAR_COUNTER_GRP)) {

				System.out
						.println(counter.getName() + "-" + counter.getValue());
			}
		}
	}
}
