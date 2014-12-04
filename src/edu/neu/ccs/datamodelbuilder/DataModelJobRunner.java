package edu.neu.ccs.datamodelbuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.neu.ccs.constants.Constants;

public class DataModelJobRunner {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 5) {
			System.err.println("Usage: datamodelbuilder <in> <test_pruned_data_out> <industry_sector_csv> <job2_output_folder> <data_model_folder>");
			System.exit(4);
		}

		Job job = new Job(conf, "Data Model Builder");

		job.setJarByClass(DataModelJobRunner.class);
		job.setMapperClass(DataModelMapper.class);
		job.setReducerClass(DataModelReducer.class);
		job.setPartitionerClass(DataModelPartitioner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setGroupingComparatorClass(DataModelGroupComparator.class);
		job.setSortComparatorClass(DataModelKeyComparator.class);

		MultipleOutputs.addNamedOutput(job, Constants.PRUNED_DATA_TAG, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Constants.TEST_DATA_TAG, TextOutputFormat.class, NullWritable.class, Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		//Setting the test_year for the data set
		job.getConfiguration().set(Constants.TEST_YEAR, "2012");
		
		// Creating hdfs file system
		FileSystem hdfs = FileSystem.get(job.getConfiguration());
		
		//Reading sectors file (secturhunt.csv)
		String secturhuntFile = otherArgs[2];
		secturhuntFile = secturhuntFile.substring(secturhuntFile.lastIndexOf("/") + 1);
		
		FileSystem s3fs = FileSystem.get(URI.create(otherArgs[2]), job.getConfiguration());
		FileStatus[] status = s3fs.listStatus(new Path(otherArgs[2]));
		readFileIntoHDFS(hdfs, s3fs, status, secturhuntFile, Constants.SECTOR_HUNT);
		
		//Reading job 2 outputs (tagSector and sectorTopTags)
		s3fs = FileSystem.get(URI.create(otherArgs[3]), job.getConfiguration());
		status = s3fs.listStatus(new Path(otherArgs[3]));
		
		// TODO names of job 2
		readFileIntoHDFS(hdfs, s3fs, status, "", Constants.TAG_SECTOR_FILE);
		readFileIntoHDFS(hdfs, s3fs, status, "", Constants.TOP_TAGS_SECTOR);		
		
		//TODO - change the name
		job.getConfiguration().set(Constants.SECOND_OUTPUT_FOLDER, otherArgs[4]);
		
		//Displaying the counters and their values
		job.waitForCompletion(true);
		
		s3fs.close();
		hdfs.close();
	}

	private static void readFileIntoHDFS(FileSystem hdfs, FileSystem s3fs,
			FileStatus[] status, String fileName, String pathToWrite)
			throws IOException {

		BufferedWriter tagSectorWriter = null;

		BufferedReader bufferedReader = null;
		Path filePath = null;

		for (int i = 0; i < status.length; i++) {

			filePath = status[i].getPath();

			if (filePath.getName().contains(fileName)) {

				bufferedReader = new BufferedReader(new InputStreamReader(
						s3fs.open(filePath)));

				tagSectorWriter = new BufferedWriter(new OutputStreamWriter(
						hdfs.create(new Path(pathToWrite))));

				String line = null;

				while ((line = bufferedReader.readLine()) != null) {

					tagSectorWriter.write(line);
					tagSectorWriter.write("\n");
				}

				bufferedReader.close();
				tagSectorWriter.close();
			}
		}
	}

}
