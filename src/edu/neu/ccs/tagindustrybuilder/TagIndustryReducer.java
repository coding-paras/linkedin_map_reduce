package edu.neu.ccs.tagindustrybuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import edu.neu.ccs.constants.Constants;

public class TagIndustryReducer extends Reducer<Text, Text, NullWritable, Text> {

	private StringBuffer buffer;
	private Set<String> industries;
	private MultipleOutputs<NullWritable, Text> multipleOutputs;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		buffer = new StringBuffer();
		industries = new HashSet<String>();
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		if (key.toString().contains(Constants.UNIQUE_INDUSTRIES_KEY_TAG)) {
			emitUniqueIndustry(key, context);
			return;
		}
		
		if (key.toString().contains(Constants.UNIQUE_LOCTIONS_KEY_TAG)) {
			emitUniqueILocation(key, context);
			return;
		}

		buffer.append(key).append(Constants.COMMA);

		for (Text text : values) {
			industries.add(text.toString());
		}

		for (String industry : industries) {
			
			buffer.append(industry).append(Constants.COMMA);
		}

		// Value contains the tag,value_list as a comma separated string
		multipleOutputs.write("tagindustry", NullWritable.get(), new Text(
				buffer.toString().substring(0, buffer.length() - 1)));

		// Clearing buffer
		buffer.delete(0, buffer.length());

		// CLearing Set
		industries.clear();
	}

	/**
	 * Emits the unique location
	 * 
	 * @param key
	 * @param context
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void emitUniqueILocation(Text key,
			Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String location = key.toString().split(Constants.COMMA)[0];
		multipleOutputs.write("location", NullWritable.get(),
				new Text(location));
	}

	/**
	 * Emits the unique industry
	 * 
	 * @param key
	 * @param context
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void emitUniqueIndustry(Text key,
			Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String industry = key.toString().split(Constants.COMMA)[0];
		multipleOutputs.write("industries", NullWritable.get(), new Text(
				industry));
	}
	
	@Override
	protected void cleanup(
			Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}