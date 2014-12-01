package edu.neu.ccs.predictor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
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

public class PredictorJobRunner {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: predictor <in> <out> <models_top_tags_sector>");
			System.exit(4);
		}

		Job job = new Job(conf, "Predictor");

		job.setJarByClass(PredictorJobRunner.class);
		job.setMapperClass(PredcitorMapper.class);
		job.setReducerClass(PredcitorReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(UserProfile.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Distributed Cache - HDFS Output from Job1
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] status = fs.listStatus(new Path(otherArgs[2]));

		BufferedWriter modelsWriter = new BufferedWriter(new FileWriter(
				Constants.MODELS));

		BufferedReader bufferedReader = null;
		Path filePath = null;

		for (int i = 0; i < status.length; i++) {

			filePath = status[i].getPath();
			if (filePath.getName().contains(Constants.DATA_MODEL_TAG)) {
				bufferedReader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
				String line = null;
				while ((line = bufferedReader.readLine()) != null) {
					modelsWriter.write(line);
					modelsWriter.write("\n");
				}
				bufferedReader.close();

			}
			else if (filePath.getName().contains(Constants.TOP_TAGS_FILE_TAG)) {
				
				// Setting distributed cache of industry to sector mapping.
				DistributedCache.addCacheFile(filePath.toUri(), job.getConfiguration());
				job.getConfiguration().set(Constants.TOP_TAGS, filePath.toString()); //DistributedCache filename				
			}
		}
		modelsWriter.close();
		
		// Displaying the counters and their values
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
