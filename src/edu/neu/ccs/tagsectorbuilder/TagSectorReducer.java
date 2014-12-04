package edu.neu.ccs.tagsectorbuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.util.UtilHelper;

public class TagSectorReducer extends Reducer<Text, Text, NullWritable, Text> {

	private Map<String, Integer> topSector;
	private Map<String, String> industryToSector;
	private Map<String, Integer> tagCounts;
	private StringBuffer buffer;
	private MultipleOutputs<NullWritable, Text> multipleOutputs;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		topSector = new HashMap<String, Integer>();
		tagCounts = new HashMap<String, Integer>();
		buffer = new StringBuffer();
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		
		Path path = new Path(Constants.SECTOR_CSV);
		Configuration conf = context.getConfiguration();
		FileSystem.get(conf).copyToLocalFile(new Path(Constants.SECTOR_CSV), path);
		
		industryToSector = UtilHelper.populateKeyValue(path.toString());
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String industry = key.toString();
		if (industry.contains(Constants.INDUSTRY_TAG)) {
			
			emitTopTagsPerSector(values, context, industryToSector.get(industry.split(Constants.COMMA)[0]));
			return;
		}
		
		emitTopSector(key, values, context);
		
		topSector.clear();
		tagCounts.clear();
	}

	private void emitTopTagsPerSector(Iterable<Text> values, Context context, String sector) 
			throws IOException, InterruptedException {
		
		if (sector == null) {
			
			return;
		}
		for (Text tag : values) {
			
			Integer count = tagCounts.get(tag.toString());
			count = count == null ? 1 : count + 1;
			
			tagCounts.put(tag.toString(), count);
		}
		
		List<Entry<String, Integer>> tagsList = new ArrayList<Entry<String, Integer>>(tagCounts.entrySet());
		Collections.sort(tagsList, 
				new Comparator<Entry<String, Integer>>() {

			@Override
			public int compare(Entry<String, Integer> entry1, Entry<String, Integer> entry2) {

				return -entry1.getValue().compareTo(entry2.getValue());
			}
		});

		finallyEmit(sector, tagsList, context);
	}
	
	private void finallyEmit(String industry, List<Entry<String, Integer>> tagsList, Context context) 
			throws IOException, InterruptedException {

		buffer.append(industry).append(Constants.COMMA);
		
		int numberOfTags = tagsList.size();
		if (numberOfTags > 5) {
			
			numberOfTags = 5;
		}

		for (int i = 0; i < numberOfTags; i++) {
			
			buffer.append(tagsList.get(i).getKey()).append(Constants.COMMA);
		}

		// Value contains the tag,value_list as a comma separated string
		multipleOutputs.write(Constants.TOP_TAGS, NullWritable.get(), 
				new Text(buffer.toString().substring(0, buffer.length() - 1)));

		// Clearing buffer
		buffer.delete(0, buffer.length());
	}
	
	private void emitTopSector(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String industries[] = null;
		String sector = null;
		Integer count = null;
		for (Text value : values) {
			
			industries = value.toString().split(Constants.COMMA);

			for (String industry_1 : industries) {
				
				sector = industryToSector.get(industry_1);
				if (sector != null) {
					
					count = topSector.get(sector);
					if (count == null) {
						
						count = 1;
						topSector.put(sector, count);
					}
					else {
						
						topSector.put(sector, count + 1);
					}
				}
			}
		}
		
		if (!topSector.isEmpty()) {
			
			emitTopSector(key.toString(), context);
		}
	}

	private void emitTopSector(String tag, Context context) throws IOException, InterruptedException {
		
		List<Entry<String, Integer>> collection = new ArrayList<Entry<String, Integer>>(
				topSector.entrySet());

		Collections.sort(collection,
				new Comparator<Entry<String, Integer>>() {

			@Override
			public int compare(Entry<String, Integer> entry1, Entry<String, Integer> entry2) {
				
				return -entry1.getValue().compareTo(entry2.getValue());
			}
		});
		
		context.write(NullWritable.get(), new Text(tag + Constants.COMMA + collection.get(0).getKey()));
	}
}