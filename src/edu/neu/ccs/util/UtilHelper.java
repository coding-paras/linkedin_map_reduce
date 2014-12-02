package edu.neu.ccs.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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

				retrieveIndustrySectorMap(industryToSector, path);
				break;
			}
		}
		return industryToSector;
	}

	public static void retrieveIndustrySectorMap(Map<String, String> industryToSector, Path filePath)
			throws IOException {
		
		BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));

		String line = null;
		String words[] = null;
		while ((line = bufferedReader.readLine()) != null) {
			
			words = line.split(Constants.COMMA);
			industryToSector.put(words[0], words[1]);
		}
		bufferedReader.close();
	}

	public static String serialize(Object obj) throws IOException {
		
		ByteArrayOutputStream b = null;
		try {
			
			b = new ByteArrayOutputStream();
			ObjectOutputStream o = new ObjectOutputStream(b);
			o.writeObject(obj);
			
			return new String(b.toByteArray());
		} finally {
			if (b != null) {
				b.close();
			}
		}
	}

	public static Object deserialize(String obj) throws IOException, ClassNotFoundException {
		
		ByteArrayInputStream b = null;
		try {
			
			b = new ByteArrayInputStream(obj.getBytes());
			ObjectInputStream o = new ObjectInputStream(b);
			
			return o.readObject();
		} finally {
			if (b != null) {
				b.close();
			}
		}
	}
}
