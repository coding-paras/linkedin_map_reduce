package edu.neu.ccs.tagsectorbuilder;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.neu.ccs.constants.Constants;

public class TagSectorMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

	}

	/**
	 * Emits (tag, industry) where tag is a skill of a title. Also, this class
	 * increases the global counters for each year.
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String stringValue = value.toString();
		String tag = stringValue.split(Constants.COMMA)[0];
		String industries = stringValue.substring(stringValue.indexOf(Constants.COMMA) + 1);

		context.write(new Text(tag), new Text(industries));

	}
}