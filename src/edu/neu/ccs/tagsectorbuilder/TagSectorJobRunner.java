package edu.neu.ccs.tagsectorbuilder;

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

		FileSystem s3fs = FileSystem.get(URI.create(otherArgs[2]),job.getConfiguration());
		//FileStatus[] status = s3fs.listStatus(new Path(otherArgs[2]));
		
		s3fs.copyFromLocalFile(new Path(otherArgs[2]), new Path(Constants.TAG_INDUSTRY_FILE));

		/*FileSystem hdfs = FileSystem.get(job.getConfiguration());
		Path filePath = null;
		BufferedReader bufferedReader = null;
		BufferedWriter tagSectorWriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(Constants.TAG_INDUSTRY_FILE))));
		
		for (int i = 0; i < status.length; i++) {

			filePath = status[i].getPath();
			if (filePath.getName().contains("sectors.csv")) {
				bufferedReader = new BufferedReader(new InputStreamReader(s3fs.open(filePath)));
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					tagSectorWriter.write(line);
					tagSectorWriter.write("\n");
				}
				bufferedReader.close();
			}
		}
		
		tagSectorWriter.close();*/

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.waitForCompletion(true);
	}
}
