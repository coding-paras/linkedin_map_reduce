package edu.neu.ccs.datamodelbuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import edu.neu.ccs.constants.Constants;

public class DataModelMapper extends Mapper<Object, Text, Text, Text> {

	private static Logger logger = Logger.getLogger(DataModelMapper.class);
	
	private Map<String, String> industryToSector;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);
		
		setupIndustryToSector(context);
	}

	/**
	 * Reads the industry to sector mapping file from distributed cache and
	 * populates a {@link Map}
	 * 
	 * @param context
	 * @throws IOException
	 */
	private void setupIndustryToSector(Context context) throws IOException {

		industryToSector = new HashMap<String, String>();

		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());

		String industrySectorFile = context.getConfiguration().get(Constants.INDUSTRY_SECTOR_FILE);
		industrySectorFile = industrySectorFile.substring(industrySectorFile.lastIndexOf("/") + 1);

		if (localFiles == null) {

			throw new RuntimeException("DistributedCache not present in HDFS");
		}

		for (Path path : localFiles) {

			if (industrySectorFile.equals(path.getName())) {

				BufferedReader bufferedReader = new BufferedReader(new FileReader(path.toString()));

				String line = null;
				String words[] = null;
				while ((line = bufferedReader.readLine()) != null) {
					words = line.split(Constants.COMMA);
					industryToSector.put(words[0], words[1]);
				}
				bufferedReader.close();				
			}
		}
	}
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		
	}
}
