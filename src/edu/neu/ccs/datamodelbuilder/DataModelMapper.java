package edu.neu.ccs.datamodelbuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.objects.Position;
import edu.neu.ccs.objects.UserProfile;

public class DataModelMapper extends Mapper<Object, Text, Text, Text> {

	private static Logger logger = Logger.getLogger(DataModelMapper.class);
	
	private Map<String, String> industryToSector;
	private Map<String, List<String>> tagToIndustries, topTagsPerIndustry;
	private String tagIndustriesFile;
	private String topTagsPerIndustryFile;
	private Gson gson;
	private Type userProfileListType;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);
		
		tagIndustriesFile = Constants.TAG_INDUSTRY_FILE + System.currentTimeMillis();
		FileSystem.get(context.getConfiguration()).copyToLocalFile(new Path(Constants.TAG_INDUSTRY_FILE), new Path(tagIndustriesFile));
		populateKeyValues(tagToIndustries, tagIndustriesFile);
		
		topTagsPerIndustryFile = Constants.TOP_5TAGS_INDUSTRY + System.currentTimeMillis();
		FileSystem.get(context.getConfiguration()).copyToLocalFile(new Path(Constants.TOP_5TAGS_INDUSTRY), new Path(topTagsPerIndustryFile));
		populateKeyValues(topTagsPerIndustry, topTagsPerIndustryFile);
		
		populateIndustryToSector(context);
		
		gson = new Gson();
		userProfileListType = new TypeToken<List<UserProfile>>() {}.getType();
	}

	/**
	 * 
	 * @param map
	 * @param fileName
	 * @throws IOException
	 */
	private void populateKeyValues(Map<String, List<String>> map, String fileName) throws IOException {
		
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
	 * @param context
	 * @throws IOException
	 */
	private void populateIndustryToSector(Context context) throws IOException {

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

		try
		{
			if (value.toString().trim().isEmpty()) {
				return;
			}

			List<UserProfile> userProfileList = gson.fromJson(value.toString(),
					userProfileListType);

			String industry = null;
			if (userProfileList == null) {
				return;
			}

			for (UserProfile userProfile : userProfileList) {

				/*industry = userProfile.getIndustry();
				if (industry == null || industry.trim().isEmpty()) {

					assignIndustry(userProfile);
				}*/
				
				assignSector(userProfile);
			}
		} catch(JsonSyntaxException jse)	{
			
			logger.error(jse);
			return;
		}

	}
	
	private void assignSector(UserProfile userProfile) {
		
		String sector = null;
		if (userProfile.getIndustry() == null || industryToSector.get(userProfile.getIndustry()) == null) {
			
			sector = getMaxSector(userProfile);
		} else {
			
			sector = industryToSector.get(userProfile.getIndustry());
		}		
		
		for (Position position : userProfile.getPositions()) {
			
			position.setSector(sector);
		}
	}
	
	private String getMaxSector(UserProfile userProfile) {
		
		List<String> tags = populateTags(userProfile);
		
		Map<String, Integer> sectorCount = new HashMap<String, Integer>();
		String maxSector = null, sector = null;
		int maxCount = -1;
		
		for (String tag : tags) {

			if (tagToIndustries.get(tag) == null) {

				continue;
			}

			for (String industry : tagToIndustries.get(tag)) {

				int count = 1;
				sector = industryToSector.get(industry);
				if (sector == null) {

					continue;
				}
				if (sectorCount.get(sector) != null) {

					count = sectorCount.get(sector) + 1;
				}
				if (count > maxCount) {

					maxCount = count;
					maxSector = sector;
				}
				sectorCount.put(sector, count);
			}
		}		
		return maxSector;
	}


	private List<String> populateTags(UserProfile userProfile) {
		List<String> tags = new ArrayList<String>();
		
		tags.addAll(tags);
		
		for(Position position: userProfile.getPositions())
		{
			tags.add(position.getTitle());
		}
		
		return tags;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		super.cleanup(context);
		
		new File(tagIndustriesFile).delete();
		new File(topTagsPerIndustryFile).delete();
		
	}
}
