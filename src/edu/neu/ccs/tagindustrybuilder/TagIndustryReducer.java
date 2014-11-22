package edu.neu.ccs.tagindustrybuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.neu.ccs.constants.Constants;

public class TagIndustryReducer extends Reducer<Text, Text, NullWritable, Text> {

	private StringBuffer buffer;
	private Set<String> industries;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		buffer = new StringBuffer();
		industries = new HashSet<String>();
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		buffer.append(key).append(Constants.COMMA);

		for (Text text : values) {
			industries.add(text.toString());
		}

		for (String industry : industries) {
			
			buffer.append(industry).append(Constants.COMMA);
		}

		// Value contains the tag,value_list as a comma separated string
		context.write(NullWritable.get(), new Text(buffer.toString().substring(0, buffer.length() - 1)));

		// Clearing buffer
		buffer.delete(0, buffer.length());
		
		// CLearing Set
		industries.clear();
	}

}