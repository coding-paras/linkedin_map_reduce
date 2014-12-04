package edu.neu.ccs.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.neu.ccs.constants.Constants;

public class UtilHelper {

	/**
	 * 
	 * @param map
	 * @param fileName
	 * @return 
	 * @throws IOException
	 */
	public static Map<String, List<String>> populateKeyValues(String fileName) throws IOException {

		Map<String, List<String>> map = new HashMap<String, List<String>>();

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
		return map;
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

		if (localFiles == null) {

			throw new RuntimeException("DistributedCache not present in HDFS");
		}
		
		String industrySectorFile = configuration.get(Constants.INDUSTRY_SECTOR_FILE);
		industrySectorFile = industrySectorFile.substring(industrySectorFile.lastIndexOf("/") + 1);
		
		for (Path path : localFiles) {

			if (industrySectorFile.equals(path.getName())) {
				
				retrieveIndustrySectorMap(industryToSector, path, configuration);
				break;
			}
		}
		return industryToSector;
	}

	public static void retrieveIndustrySectorMap(Map<String, String> industryToSector, Path filePath, Configuration configuration)
			throws IOException {
		FileSystem fs = FileSystem.get(URI.create(filePath.toString()), configuration);
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(filePath)));

		String line = null;
		String words[] = null;
		while ((line = bufferedReader.readLine()) != null) {
			
			words = line.split(Constants.COMMA);
			industryToSector.put(words[0], words[1]);
		}
		bufferedReader.close();
	}
}
