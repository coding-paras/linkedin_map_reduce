package edu.neu.ccs.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
}
