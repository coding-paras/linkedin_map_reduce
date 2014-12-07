package edu.neu.ccs.employeespersector;

import java.io.IOException;
import java.lang.reflect.Type;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.objects.Position;
import edu.neu.ccs.objects.UserProfile;

public class PlainJobMapper extends Mapper<Object, Text, Text, UserProfile> {

	private Gson gson;
	private Type userProfileType;
	int yearToBeConsidered;

	private static Logger logger = Logger.getLogger(PlainJobMapper.class);

	/**
	 * Initializing Gson, setting up type for de-serializing.
	 */
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		gson = new Gson();
		userProfileType = new TypeToken<UserProfile>(){}.getType();
		yearToBeConsidered = Integer.parseInt(context.getConfiguration().get("year"));
	}

	@Override
	protected void map(Object key, Text value, Context context)throws IOException, InterruptedException {

		try {
			if (value.toString().trim().isEmpty()) {
				return;
			}

			UserProfile userProfile = gson.fromJson(value.toString(),userProfileType);

			if (userProfile == null) {
				return;
			}

			boolean toBeConsidered = false;
			int startYear, endYear;
			String sector = userProfile.getIndustry();
			if (sector == null || sector.isEmpty() || sector.equals("null")) {
				
				return;
			}
			
			for (Position position : userProfile.getPositions()) {

				if (position.getStartDate() == null) {
					
					continue;
				}
				
				startYear = Integer.parseInt(position.getStartDate().split(Constants.DATE_DELIMITER_1)[0]);
				startYear = (startYear < Constants.START_YEAR ? Constants.START_YEAR : startYear);
				endYear = (position.getEndDate() == null ? Constants.END_YEAR : 
					Integer.parseInt(position.getEndDate().split(Constants.DATE_DELIMITER_1)[0]));

				if (startYear <= yearToBeConsidered && yearToBeConsidered <= endYear){
					
					toBeConsidered = true;
					break;
				}
			}
			
			if (!toBeConsidered) {

				return;
			}
			
			context.write(new Text(sector), userProfile);

		} catch (JsonSyntaxException jse) {
			logger.error(jse);
			return;
		}
	}
}
