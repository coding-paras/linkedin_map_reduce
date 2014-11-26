package edu.neu.ccs.datamodelbuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
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
		
		Map<String, String> industryToSector = populateIndustryToSector(job.getConfiguration());
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
		
		//Displaying the counters and their values
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	private static void createTopIndustriesFile(Map<String, Map<String, Integer>> topTagsSector) throws IOException {
		BufferedWriter tagSectorWriter = new BufferedWriter(new FileWriter(
				Constants.TOP_TAGS_SECTOR));

		Map<String, Integer> tags = null;
		List<Map.Entry<String, Integer>> topTags = new ArrayList<Map.Entry<String, Integer>>();
		
		for (Map.Entry<String, Map<String, Integer>> entry : topTagsSector
				.entrySet()) {

			tags = entry.getValue();

			topTags.addAll(tags.entrySet());

			Collections.sort(topTags,
					new Comparator<Map.Entry<String, Integer>>() {

						@Override
						public int compare(Entry<String, Integer> entry1,
								Entry<String, Integer> entry2) {

							return -entry1.getValue().compareTo(
									entry2.getValue());
						}
					});

			if (topTags.size() >= 5) {
				writeTofile(entry.getKey(), tagSectorWriter, 5, topTags);
			} else {
				writeTofile(entry.getKey(), tagSectorWriter, topTags.size(), topTags);
			}

			topTags.clear();
		}

		tagSectorWriter.close();
	}

	private static void writeTofile(String sector, BufferedWriter tagSectorWriter, int numberOfSkills, List<Entry<String, Integer>> topTags) throws IOException {
		
		StringBuffer buffer = new StringBuffer();
		buffer.append(sector).append(Constants.COMMA);
		
		for (int i = 0; i < numberOfSkills; i++) {
			buffer.append(topTags.get(i).getKey()).append(Constants.COMMA);
		}
		
		tagSectorWriter.write(buffer.toString().substring(0, buffer.length() - 1));
		tagSectorWriter.write("\n");
	}

	/**
	 * Reads the industry to sector mapping file from distributed cache and
	 * populates a {@link Map}
	 * 
	 * @param configuration
	 * @throws IOException
	 */
	private static Map<String, String> populateIndustryToSector(Configuration configuration) throws IOException {

		Map<String, String> industryToSector = new HashMap<String, String>();

		Path[] localFiles = DistributedCache.getLocalCacheFiles(configuration);

		String industrySectorFile = configuration.get(Constants.INDUSTRY_SECTOR_FILE);
		industrySectorFile = industrySectorFile.substring(industrySectorFile.lastIndexOf("/") + 1);

		if (localFiles == null) {

			throw new RuntimeException("DistributedCache not present in HDFS");
		}

		for (Path path : localFiles) {

			if (industrySectorFile.equals(path.getName())) {

				BufferedReader bufferedReader = new BufferedReader(new FileReader(path.toString()));

				String line = null;
				String words[] = null;
				while ((line = bufferedReader.readLine()) != null) {
					words = line.split(Constants.COMMA);
					industryToSector.put(words[0], words[1]);
				}
				bufferedReader.close();				
			}
		}
		return industryToSector;
	}

}
