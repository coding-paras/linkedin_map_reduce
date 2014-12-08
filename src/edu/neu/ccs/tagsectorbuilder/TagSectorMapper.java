package edu.neu.ccs.tagsectorbuilder;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.util.UtilHelper;

public class TagSectorMapper extends Mapper<Object, Text, Text, Text> {

	public String sectorFileName;
	private Map<String, String> industryToSector;
	private StringBuffer sectorBuffer;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		sectorFileName = Constants.SECTOR_CSV + System.currentTimeMillis();
		Path path = new Path(sectorFileName);
		Configuration conf = context.getConfiguration();
		FileSystem.get(conf).copyToLocalFile(new Path(Constants.SECTOR_CSV), path);
		
		industryToSector = UtilHelper.populateKeyValue(path.toString());
		
		sectorBuffer = new StringBuffer();
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

		Text outKey = new Text(tag);
		
		for (String industry : industries.split(Constants.COMMA)) {
			
			String sector = industryToSector.get(industry);
			if (sector == null || sectorBuffer.toString().contains(sector)) {
				
				continue;
			}
			
			sectorBuffer.append(sector).append(Constants.COMMA); 
			context.write(new Text(sector + Constants.COMMA + Constants.SECTOR_TAG), outKey);
		}

		context.write(outKey, new Text(sectorBuffer.substring(0, sectorBuffer.length() - 1)));
		
		//Clearing the buffer
		sectorBuffer.delete(0, sectorBuffer.length());
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		super.cleanup(context);
		new File(sectorFileName).delete();
	}
}