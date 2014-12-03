package edu.neu.ccs.datamodelbuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import edu.neu.ccs.objects.UserProfile;
import edu.neu.ccs.util.UtilHelper;

public class DataModelJobRunner {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 5) {
			System.err.println("Usage: datamodelbuilder <in> <out> <industry_sector_csv> <output_job1_folder> <top_tag_sector_folder>");
			System.exit(4);
		}

		Job job = new Job(conf, "Data Model Builder");

		job.setJarByClass(DataModelJobRunner.class);
		job.setMapperClass(DataModelMapper.class);
		job.setReducerClass(DataModelReducer.class);
		job.setPartitionerClass(DataModelPartitioner.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(UserProfile.class);
		
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setSortComparatorClass(KeyComparator.class);

		MultipleOutputs.addNamedOutput(job, Constants.PRUNED_DATA_TAG, TextOutputFormat.class, NullWritable.class, Text.class);
		//MultipleOutputs.addNamedOutput(job, Constants.DATA_MODEL_TAG, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Constants.TEST_DATA_TAG, TextOutputFormat.class, NullWritable.class, Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Setting distributed cache of industry to sector mapping.
		DistributedCache.addCacheFile(new URI(otherArgs[2]), job.getConfiguration());
		
		job.getConfiguration().set(Constants.INDUSTRY_SECTOR_FILE, otherArgs[2]); //DistributedCache filename
		
		//Setting the test_year for the data set
		job.getConfiguration().set(Constants.TEST_YEAR, "2012");
		
		//Distributed Cache - HDFS Output from Job1
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] status = fs.listStatus(new Path(otherArgs[3]));
		
		Map<String, String> industryToSector = new HashMap<String, String>();
		UtilHelper.retrieveIndustrySectorMap(industryToSector, new Path(otherArgs[2]));
		
		Map<String, Map<String, Integer>> topTagsSector = new HashMap<String, Map<String, Integer>>();
	
		BufferedWriter tagIndustryWriter = new BufferedWriter(new FileWriter(Constants.TAG_INDUSTRY_FILE));

		BufferedReader bufferedReader = null;
		Path filePath = null;
		Map<String, Integer> tags;
		
		for (int i=0;i<status.length;i++){
			
			filePath = status[i].getPath();
			bufferedReader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
			
			if (filePath.getName().contains(Constants.TAG_INDUSTRY)) {
				
				String line;
				while ((line=bufferedReader.readLine()) != null){
					
					tagIndustryWriter.write(line);
					tagIndustryWriter.write("\n");
				}
				bufferedReader.close();
				
			} else if (filePath.getName().contains(Constants.TOP_TAGS)) {
				
				String line;
				String values[];
				while ((line=bufferedReader.readLine()) != null){
					
					values = line.split(Constants.COMMA);
					
					String sector = industryToSector.get(values[0]);

					if (sector ==  null) {
						continue;
					}
					
					tags = topTagsSector.get(sector);
					
					if (tags == null) {
						tags = new HashMap<String, Integer>();
						topTagsSector.put(sector, tags);
					}
					
					for (int j = 1; j < values.length; j++) {
						String tag = values[j];
						int count = 1;
						if (tags.get(tag) != null) {
							count = tags.get(tag) + 1;
						}
						tags.put(tag, count);				
					}
				}
				bufferedReader.close();
			}
		}
		
		tagIndustryWriter.close();
		
		createTopIndustriesFile(topTagsSector);
		
		fs.copyFromLocalFile(new Path(Constants.TOP_TAGS_SECTOR), new Path(otherArgs[4] + File.separator + Constants.TOP_TAGS_FILE_TAG));
		
		fs.close();
		
		//TODO - change the name
		job.getConfiguration().set(Constants.SECOND_OUTPUT_FOLDER, otherArgs[4]);
		
		//Displaying the counters and their values
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	private static void createTopIndustriesFile(Map<String, Map<String, Integer>> topTagsSector) throws IOException {
		
		BufferedWriter tagSectorWriter = new BufferedWriter(new FileWriter(Constants.TOP_TAGS_SECTOR));

		Map<String, Integer> tags = null;
		List<Map.Entry<String, Integer>> topTags = new ArrayList<Map.Entry<String, Integer>>();
		
		for (Map.Entry<String, Map<String, Integer>> entry : topTagsSector.entrySet()) {

			tags = entry.getValue();

			topTags.addAll(tags.entrySet());

			Collections.sort(topTags,
					new Comparator<Map.Entry<String, Integer>>() {

						@Override
						public int compare(Entry<String, Integer> entry1, Entry<String, Integer> entry2) {

							return -entry1.getValue().compareTo(entry2.getValue());
						}
					});

			if (topTags.size() >= 5) {
				writeTopTagsPerSectorToFile(entry.getKey(), tagSectorWriter, 5, topTags);
			} else {
				writeTopTagsPerSectorToFile(entry.getKey(), tagSectorWriter, topTags.size(), topTags);
			}

			topTags.clear();
		}

		tagSectorWriter.close();
	}
	
	private static void writeTopTagsPerSectorToFile(String sector, final BufferedWriter tagSectorWriter, int numberOfSkills, 
			List<Entry<String, Integer>> topTags) throws IOException {
		
		StringBuffer buffer = new StringBuffer();
		buffer.append(sector).append(Constants.COMMA);
		
		for (int i = 0; i < numberOfSkills; i++) {
			buffer.append(topTags.get(i).getKey()).append(Constants.COMMA);
		}
		
		tagSectorWriter.write(buffer.toString().substring(0, buffer.length() - 1));
		tagSectorWriter.write("\n");
	}
}
