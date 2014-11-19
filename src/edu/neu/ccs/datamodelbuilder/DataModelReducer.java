package edu.neu.ccs.datamodelbuilder;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataModelReducer extends Reducer<Text, Text, NullWritable, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

	}

}