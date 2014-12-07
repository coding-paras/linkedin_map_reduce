package edu.neu.ccs.predictor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
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

public class PredictorJobRunner {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 4) {
			System.err.println("Usage: predictor <in> <out> <models> <top_tags_sector>");
			System.exit(3);
		}

		Job job = new Job(conf, "Predictor");

		job.setJarByClass(PredictorJobRunner.class);
		job.setMapperClass(PredictorMapper.class);
		job.setReducerClass(PredictorReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		// Creating hdfs file system
		FileSystem hdfs = FileSystem.get(job.getConfiguration());
		
		// Reading models
		FileSystem s3fs = FileSystem.get(URI.create(otherArgs[2]),job.getConfiguration());
		FileStatus[] status = s3fs.listStatus(new Path(otherArgs[2]));
		
		Path filePath = null;
		for (int i = 0; i < status.length; i++) {
			filePath = status[i].getPath();
			s3fs.copyToLocalFile(new Path(URI.create(otherArgs[2] + filePath.getName())), new Path(Constants.MODELS + filePath.getName() + "_"));
			hdfs.copyFromLocalFile(new Path(Constants.MODELS + filePath.getName() + "_"), new Path(Constants.MODELS + filePath.getName()));
			new File(Constants.MODELS + filePath.getName() + "_").delete();
		}		
		
		//readModelFileIntoHDFS(hdfs, s3fs, status);
		
		// Reading job 2 outputs (tagSector and sectorTopTags)
		s3fs = FileSystem.get(URI.create(otherArgs[3]), job.getConfiguration());
		status = s3fs.listStatus(new Path(otherArgs[3]));
		readFileIntoHDFS(hdfs, s3fs, status, Constants.TOP_TAGS , Constants.TOP_TAGS_SECTOR);	
		
		// Displaying the counters and their values
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private static void readFileIntoHDFS(FileSystem hdfs, FileSystem s3fs,
			FileStatus[] status, String fileName, String pathToWrite)
			throws IOException {

		BufferedWriter tagSectorWriter =  new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(pathToWrite))));

		BufferedReader bufferedReader = null;
		Path filePath = null;
		
		for (int i = 0; i < status.length; i++) {

			filePath = status[i].getPath();

			if (filePath.getName().contains(fileName)) {

				bufferedReader = new BufferedReader(new InputStreamReader(
						s3fs.open(filePath)));



				String line = null;

				while ((line = bufferedReader.readLine()) != null) {

					tagSectorWriter.write(line);
					tagSectorWriter.write("\n");
				}

				bufferedReader.close();
			}
		}
		tagSectorWriter.close();
	}
}
