package edu.neu.ccs.datamodelbuilder;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class DataModelMapper extends Mapper<Object, Text, Text, Text> {

	private static Logger logger = Logger.getLogger(DataModelMapper.class);

	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		super.setup(context);
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
	}
}
