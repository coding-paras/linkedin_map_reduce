package edu.neu.ccs.tagindustrybuilder;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Sets;

import edu.neu.ccs.constants.Constants;

public class TagIndustryReducer extends Reducer<Text, Text, NullWritable, Text> {

	private StringBuffer buffer;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		buffer = new StringBuffer();
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		buffer.append(key).append(Constants.COMMA);

		for (Text industry : Sets.newHashSet(values)) {
			
			buffer.append(industry).append(Constants.COMMA);
		}

		// Value contains the tag,value_list as a comma separated string
		context.write(NullWritable.get(), new Text(buffer.toString().substring(0, buffer.length() - 1)));

		// Clearing buffer
		buffer.delete(0, buffer.length());
	}

}