package edu.neu.ccs.tagsectorbuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.neu.ccs.constants.Constants;

public class TagSectorReducer extends Reducer<Text, Text, NullWritable, Text> {

	private Map<String, Integer> topSector;
	private Map<String, String> industryToSector;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		topSector = new HashMap<String, Integer>();
		Path path = new Path("/tmp/sectors.csv");
		FileSystem.get(context.getConfiguration()).copyToLocalFile(new Path(Constants.TAG_INDUSTRY_FILE), path);
		populateindustryToSector(path);
	}

	private void populateindustryToSector(Path path) throws IOException {
		industryToSector = new HashMap<String, String>();
		BufferedReader bufferedReader =  new BufferedReader(new FileReader(path.toString()));
		
		String line = null;
		String words[] = null;
		while ((line = bufferedReader.readLine()) != null) {
			
			words = line.split(Constants.COMMA);
			industryToSector.put(words[0], words[1]);
		}
		bufferedReader.close();		
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String industries[] = null;
		String sector = null;
		Integer count = null;
		for (Text value : values) {
			industries = value.toString().split(Constants.COMMA);

			for (String industry : industries) {
				sector = industryToSector.get(industry);
				if (sector != null) {
					count = topSector.get(sector);
					if (count == null) {
						count = 1;
						topSector.put(sector, count);
					} else {
						topSector.put(sector, count + 1);
					}
				}
			}
		}
		
		if (!topSector.isEmpty()) {
			emitTopSector(key.toString(), context);
		}
		topSector.clear();
	}

	private void emitTopSector(String tag, Context context) throws IOException, InterruptedException {
		List<Map.Entry<String, Integer>> collection = new ArrayList<Map.Entry<String, Integer>>(
				topSector.entrySet());

		Collections.sort(collection,
				new Comparator<Map.Entry<String, Integer>>() {

					@Override
					public int compare(Entry<String, Integer> entry1, Entry<String, Integer> entry2) {
						return -entry1.getValue().compareTo(entry2.getValue());
					}
				});
		
		context.write(NullWritable.get(), new Text(tag + Constants.COMMA + collection.get(0).getKey()));
	}

}