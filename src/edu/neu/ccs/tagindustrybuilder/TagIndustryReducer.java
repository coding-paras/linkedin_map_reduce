package edu.neu.ccs.tagindustrybuilder;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TagIndustryReducer extends Reducer<Text, Text, NullWritable, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> iterable, Context context)
			throws IOException, InterruptedException {
	}

}