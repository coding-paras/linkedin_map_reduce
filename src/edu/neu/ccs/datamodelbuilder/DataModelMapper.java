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
	private Map<String, String> tagToSector;
	private String tagSectorFile;
	private Gson gson;
	private Type userProfileListType;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);
		
		// Reading tag to sector file.
		Path tagToSectorPath = new Path(Constants.TAG_SECTOR_FILE + System.currentTimeMillis());
		FileSystem.get(context.getConfiguration()).copyToLocalFile(new Path(Constants.TAG_SECTOR_FILE), tagToSectorPath);
		poplulateMap(tagToSectorPath, tagToSector);
		
		// Reading tag to sector file.
		Path industrySectorPath = new Path(Constants.SECTOR_HUNT+ System.currentTimeMillis());
		FileSystem.get(context.getConfiguration()).copyToLocalFile(new Path(Constants.SECTOR_HUNT), industrySectorPath);
		poplulateMap(industrySectorPath, industryToSector);	
		
		gson = new Gson();
		userProfileListType = new TypeToken<List<UserProfile>>() {}.getType();
	}
	
	private void poplulateMap(Path path, Map<String, String> map) throws IOException {
		map = new HashMap<String, String>();
		BufferedReader bufferedReader = new BufferedReader(new FileReader(path.toString()));

		String line = null;
		String values[] = null;
		
		while ((line = bufferedReader.readLine()) != null) {
			values = line.split(Constants.COMMA);
			map.put(values[0], values[1]);
		}
		
		bufferedReader.close();
	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		try
		{
			if (value.toString().trim().isEmpty()) {
				return;
			}

			List<UserProfile> userProfileList = gson.fromJson(value.toString(),	userProfileListType);

			if (userProfileList == null) {
				return;
			}

			for (UserProfile userProfile : userProfileList) {

				assignSectorAndEmitData(userProfile, context);
			}
		} catch(JsonSyntaxException jse)	{
			
			logger.error(jse);
			return;
		}

	}
	
	private void assignSectorAndEmitData(UserProfile userProfile, Context context) throws IOException, InterruptedException {
		
		String sector = null;
		int finalStartYear = Integer.MAX_VALUE, finalEndYear = Integer.MIN_VALUE;
		int startYear, endYear;
		Map<String, List<Position>> positionsPerYearSector = new HashMap<String, List<Position>>();
		if (userProfile.getIndustry() == null || industryToSector.get(userProfile.getIndustry()) == null) {
			
			sector = getMaxSector(userProfile);
		} else {
			
			sector = industryToSector.get(userProfile.getIndustry());
		}		
		
		//removing + from the number of connections //TODO - move to job 1
		if (userProfile.getNumOfConnections() != null) {
			
			userProfile.setNumOfConnections(userProfile.getNumOfConnections().replaceAll(Constants.PLUS, Constants.EMPTY_STRING));
		} 
		else {
			
			userProfile.setNumOfConnections("0");
		}
		//setting the sector/industry
		userProfile.setIndustry(sector);
		for (Position position : userProfile.getPositions()) {

			if (position.getStartDate() == null) {
				
				//context.getCounter(Constants.DATA_MODEL, Constants.START_YEAR_NULL);
				continue;
			}
			//Replacing "/" with "-" in the dates //TODO - move to job 1
			position.setStartDate(position.getStartDate().replaceAll(Constants.DATE_DELIMITER_2, Constants.DATE_DELIMITER_1));
			if (position.getEndDate() != null) {
				
				position.setEndDate(position.getEndDate().replaceAll(Constants.DATE_DELIMITER_2, Constants.DATE_DELIMITER_1));
			}
			
			startYear = Integer.parseInt(position.getStartDate().split(Constants.DATE_DELIMITER_1)[0]);
			startYear = (startYear < Constants.START_YEAR ? Constants.START_YEAR : startYear);
			endYear = (position.getEndDate() == null ? Constants.END_YEAR : 
				Integer.parseInt(position.getEndDate().split(Constants.DATE_DELIMITER_1)[0]));
			
			if (startYear < finalStartYear) {
				finalStartYear = startYear;
			}
			
			if (endYear > finalEndYear) {
				finalEndYear = endYear;
			}
			
			// Pruning
			position.setSummary(null);
			
			List<Position> positions = null;
			String key = null;
			for (int i = startYear; i <= endYear; i++) {
				
				key = i + Constants.COMMA + sector;
				positions = new ArrayList<Position>();
				if (positionsPerYearSector.containsKey(key)) {
					
					positions = positionsPerYearSector.get(key);
				}
				positions.add(position);
				positionsPerYearSector.put(key, positions);
			}
		}
		
		//TODO - modify calculating experience to add the stints at each of the positions
		UserProfile newUserProfile = null;
		String key = null;
		for (int i = finalStartYear; i <= finalEndYear; i++) {
			
			// Emitting user profile for each year
			key = i + Constants.COMMA + sector;
			newUserProfile = new UserProfile(userProfile.getFirstName(), userProfile.getLastName(), userProfile.getNumOfConnections(),
					userProfile.getIndustry(), userProfile.getLocation(), userProfile.getSkillSet(), positionsPerYearSector.get(key));
			newUserProfile.setRelevantExperience(i - finalStartYear);
			context.write(new Text(key), new Text(gson.toJson(newUserProfile)));
		}
		
		//Pruned data
		context.write(new Text(Constants.PRUNED_DATA), new Text(gson.toJson(userProfile)));
	}
	
	private String getMaxSector(UserProfile userProfile) {
		
		List<String> tags = populateTags(userProfile);
		
		Map<String, Integer> sectorCount = new HashMap<String, Integer>();
		String maxSector = null, sector = null;
		int maxCount = -1;
		
		for (String tag : tags) {

			sector = tagToSector.get(tag);
			if (sector == null) {

				continue;
			}

			int count = 1;
			if (sectorCount.get(sector) != null) {

				count = sectorCount.get(sector) + 1;
			}
			if (count > maxCount) {

				maxCount = count;
				maxSector = sector;
			}
			sectorCount.put(sector, count);
		}		
		return maxSector;
	}


	private List<String> populateTags(UserProfile userProfile) {
		List<String> tags = new ArrayList<String>();
		
		tags.addAll(userProfile.getSkillSet());
		
		for(Position position: userProfile.getPositions())
		{
			tags.add(position.getTitle());
		}
		
		return tags;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		super.cleanup(context);
		
		new File(tagSectorFile).delete();		
	}
}
