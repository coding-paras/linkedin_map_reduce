package edu.neu.ccs.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.neu.ccs.constants.Constants;

public class UtilHelper {
	
	/**
	 * 
	 * @param map
	 * @param fileName
	 * @throws IOException
	 */
	public static void populateKeyValues(Map<String, List<String>> map, String fileName) throws IOException {
		
		map = new HashMap<String, List<String>>();
		
		BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
		
		String line;
		String[] attributes = null;
		List<String> values = null;
		while((line = bufferedReader.readLine()) != null) {
			
			attributes = line.split(Constants.COMMA);
			
			values = new ArrayList<String>();
			for (int i = 1; i < attributes.length; i++) {
				
				values.add(attributes[i]);
			}
			
			map.put(attributes[0], values);
		}
		
		bufferedReader.close();
	}
	
	/**
	 * Reads the industry to sector mapping file from distributed cache and
	 * populates a {@link Map}
	 * 
	 * @param configuration
	 * @throws IOException
	 */
	public static Map<String, String> populateIndustryToSector(Configuration configuration) throws IOException {

		Map<String, String> industryToSector = new HashMap<String, String>();

		Path[] localFiles = DistributedCache.getLocalCacheFiles(configuration);

		String industrySectorFile = configuration.get(Constants.INDUSTRY_SECTOR_FILE);
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
		return industryToSector;
	}
}
