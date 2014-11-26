package edu.ccs.sector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

public class IndustryToSectorProducer {
	
	private static int count;
	
	public static void main(String[] args) throws IOException {
		
		count = 0;
		new IndustryToSectorProducer().getSectors(args[0]);		
		System.out.println(count);
	}

	public void getSectors(String folder) throws IOException {
		
		Map<String, Integer> industries = new LinkedHashMap<String, Integer>();
		
		Iterator<File> it = FileUtils.iterateFiles(new File(folder), null, false);
		File currentFile = null;
        while(it.hasNext()){
        	currentFile = it.next();
            populateIndustries(currentFile, industries);
        }
        
        List<Map.Entry<String, Integer>> industryCollection = new ArrayList<Map.Entry<String,Integer>>(industries.entrySet());
        
        Collections.sort(industryCollection, new Comparator<Map.Entry<String, Integer>>() {

			@Override
			public int compare(Entry<String, Integer> entry1,
					Entry<String, Integer> entry2) {
				
				return - entry1.getValue().compareTo(entry2.getValue());
			}
		});
        
        for (Entry<String, Integer> entry : industryCollection) {
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
        
	}

	private void populateIndustries(final File currentFile,
			final Map<String, Integer> industries) throws IOException {
		
		BufferedReader bufferedReader = new BufferedReader(new FileReader(currentFile));
		
		String line = null;
		String words[] = null;
				
		while ((line = bufferedReader.readLine()) != null) {
			count++;
			
			words = line.split(" ");
			
			for (int i = 0; i < words.length; i++) {
				words[i] = words[i].trim();
				if (words[i].isEmpty()) {
					continue;
				}
				
				if (industries.containsKey(words[i])) {
					industries.put(words[i], industries.get(words[i]) + 1);
				}
				else
				{
					industries.put(words[i], 1);
				}
			}
		}
		
		bufferedReader.close();
	}

}
