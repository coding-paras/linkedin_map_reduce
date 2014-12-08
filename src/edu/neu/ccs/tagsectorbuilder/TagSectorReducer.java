package edu.neu.ccs.tagsectorbuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
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

public class TagSectorReducer extends Reducer<Text, Text, NullWritable, Text> {

	private Map<String, Integer> topSector;
	private Map<String, Integer> tagCounts;
	private StringBuffer buffer;
	private MultipleOutputs<NullWritable, Text> multipleOutputs;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		topSector = new HashMap<String, Integer>();
		tagCounts = new HashMap<String, Integer>();
		buffer = new StringBuffer();
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String sector = key.toString();
		if (sector.contains(Constants.SECTOR_TAG)) {
			
			emitTopTagsPerSector(values, context, sector.split(Constants.COMMA)[0]);
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
	
	private void finallyEmit(String sector, List<Entry<String, Integer>> tagsList, Context context) 
			throws IOException, InterruptedException {

		buffer.append(sector).append(Constants.COMMA);
		
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
		
		String sectors[] = null;
		Integer count = null;
		for (Text value : values) {
			
			sectors = value.toString().split(Constants.COMMA);

			for (String sector : sectors) {
				
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
		
		multipleOutputs.write(Constants.TAG_SECTOR, NullWritable.get(), 
				new Text(tag + Constants.COMMA + collection.get(0).getKey()));
		
		testWriteToS3(context.getConfiguration());
	}
	
	private void testWriteToS3(Configuration configuration) throws IOException {

		FileSystem s3fs = FileSystem.get(URI.create("s3://linkedin.output/datamodels"), configuration);
		
		Path src = new Path("/tmp/test" + System.currentTimeMillis());
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(src.toString()));
		
		bufferedWriter.write("Garbage");
		bufferedWriter.close();
		
		
		s3fs.copyFromLocalFile(src, new Path("s3://linkedin.output/datamodels" + File.separator + src.toString()));
		new File(src.toString()).delete();
		
	}

	@Override
	protected void cleanup(Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		super.cleanup(context);
		multipleOutputs.close();
	}
}