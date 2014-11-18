package edu.neu.ccs.tagindustrybuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.neu.ccs.constants.Constants;

public class TagIndustryReducer extends Reducer<Text, Text, NullWritable, Text> {

	private Set<String> uniqueindusteries;
	private StringBuffer buffer;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		uniqueindusteries = new HashSet<String>();
		buffer = new StringBuffer();
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text industry : values) {
			uniqueindusteries.add(industry.toString());
		}

		buffer.append(key).append(Constants.COMMA);

		for (String industry : uniqueindusteries) {
			buffer.append(industry).append(Constants.COMMA);
		}

		// Value contains the tag,value_list as a comma separated string
		context.write(NullWritable.get(),
				new Text(buffer.toString().substring(0, buffer.length() - 1)));

		// Clearing buffer
		buffer.delete(0, buffer.length());

		// Clearing Set
		uniqueindusteries.clear();
	}

}