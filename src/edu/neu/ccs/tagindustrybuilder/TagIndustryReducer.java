package edu.neu.ccs.tagindustrybuilder;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.neu.ccs.constants.Constants;

public class TagIndustryReducer extends Reducer<Text, Text, NullWritable, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		StringBuffer buffer = new StringBuffer();
		
		buffer.append(key).append(Constants.COMMA);
		for (Text text : values) {
			
			buffer.append(text).append(Constants.COMMA);
		}
		
		//Value contains the tag,value_list as a comma separated string
		context.write(NullWritable.get(), new Text(buffer.toString().substring(0, buffer.length() - 1)));
	}

}