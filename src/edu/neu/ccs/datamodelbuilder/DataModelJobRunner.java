package edu.neu.ccs.datamodelbuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.URI;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.neu.ccs.constants.Constants;

public class DataModelJobRunner {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();

		if (otherArgs.length != 4) {
			System.err.println("Usage: datamodelbuilder <in> <out> <cache> <output_job1_folder>");
			System.exit(4);
		}

		Job job = new Job(conf, "Data Model Builder");

		job.setJarByClass(DataModelJobRunner.class);
		job.setMapperClass(DataModelMapper.class);
		job.setReducerClass(DataModelReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		MultipleOutputs.addNamedOutput(job, Constants.PRUNED_DATA_TAG, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Constants.DATA_MODEL_TAG, TextOutputFormat.class, NullWritable.class, Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Setting distributed cache of industry to sector mapping.
		DistributedCache.addCacheFile(new URI(otherArgs[2]), job.getConfiguration());
		
		job.getConfiguration().set(Constants.INDUSTRY_SECTOR_FILE, otherArgs[2]); //DistributedCache filename
		
		//Distributed Cache - HDFS Output from Job1
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] status = fs.listStatus(new Path(otherArgs[3]));
	
		BufferedWriter tagIndustryWriter = new BufferedWriter(new FileWriter(Constants.TAG_INDUSTRY_FILE));
		BufferedWriter top5TagsPerIndustryWriter = new BufferedWriter(new FileWriter(Constants.TOP_TAGS_SECTOR));
		BufferedWriter bufferedWriter = null;
		BufferedReader bufferedReader = null;
		Path filePath = null;
		for (int i=0;i<status.length;i++){
			
			filePath = status[i].getPath();
			bufferedReader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
			
			if (filePath.getName().contains(Constants.TAG_INDUSTRY)) {
				
				bufferedWriter = tagIndustryWriter;
			} else if (filePath.getName().contains(Constants.TOP_TAGS)) {
				
				bufferedWriter = top5TagsPerIndustryWriter;
			}
			String line;
			while ((line=bufferedReader.readLine()) != null){
				
				bufferedWriter.write(line);
				bufferedWriter.write("\n");
			}
			bufferedReader.close();
		}
		tagIndustryWriter.close();
		top5TagsPerIndustryWriter.close();
		
		//Displaying the counters and their values
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
