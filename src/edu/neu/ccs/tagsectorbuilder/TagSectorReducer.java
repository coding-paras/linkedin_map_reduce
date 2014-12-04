package edu.neu.ccs.tagsectorbuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.util.UtilHelper;

public class TagSectorReducer extends Reducer<Text, Text, NullWritable, Text> {

	private Map<String, Integer> topSector;
	private Map<String, String> industryToSector;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		topSector = new HashMap<String, Integer>();
		industryToSector = UtilHelper.populateIndustryToSector(context
				.getConfiguration());
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