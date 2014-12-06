package edu.neu.ccs.sectortopskills;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

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

			StartEndYear startEndYear = getStartEndDate(userProfile);

			if (startEndYear == null || !userToBeConsidered(startEndYear, userProfile)) {
				return;
			}

			context.write(new Text(userProfile.getIndustry()), userProfile);

		} catch (JsonSyntaxException jse) {
			logger.error(jse);
			return;
		}
	}

	private StartEndYear getStartEndDate(UserProfile userProfile) {
		List<Position> positions = userProfile.getPositions();

		if (positions == null) {
			return null;
		}

		int finalStartYear = Integer.MAX_VALUE;
		int finalEndYear = Integer.MIN_VALUE;
		int startYear = 0, endYear = 0;

		String startDate = null, endDate = null;
		for (Position position : positions) {
			try {
				startDate = position.getStartDate();
				endDate = position.getEndDate();

				if (startDate != null) {
					startYear = Integer.parseInt(startDate.replaceAll(Constants.DATE_DELIMITER_2,Constants.DATE_DELIMITER_1).split(Constants.DATE_DELIMITER_1)[0]);
					if (startYear < finalStartYear) {
						finalStartYear = startYear;
					}
				}

				if (endDate != null) {
					endYear = Integer.parseInt(startDate.replaceAll(Constants.DATE_DELIMITER_2,Constants.DATE_DELIMITER_1).split(Constants.DATE_DELIMITER_1)[0]);
					if (endYear > finalEndYear) {
						finalEndYear = endYear;
					}
				}
			} catch (NumberFormatException nme) {
				logger.error(nme);
				continue;
			}
		}

		if (finalStartYear == 0 || finalStartYear == Integer.MAX_VALUE) {
			return null;
		}

		if (finalStartYear < Constants.START_YEAR) {
			finalStartYear = Constants.START_YEAR;
		}

		if (finalEndYear == 0 || finalEndYear == Integer.MIN_VALUE) {
			finalEndYear = Constants.END_YEAR;
		}

		return new StartEndYear(finalStartYear, finalEndYear);
	}

	private boolean userToBeConsidered(StartEndYear startEndYear, UserProfile userProfile) {

		if (startEndYear.getStartYear() <= yearToBeConsidered
				&& startEndYear.getEndYear() >= yearToBeConsidered
				&& userProfile.getIndustry() != null
				&& userProfile.getLocation() != null) {
			return true;
		}
		return false;
	}

	public static class StartEndYear {
		private int startYear;
		private int endYear;

		public StartEndYear() {

		}

		public StartEndYear(int startYear, int endYear) {
			this.startYear = startYear;
			this.endYear = endYear;
		}

		public int getStartYear() {
			return startYear;
		}

		public void setStartYear(int startYear) {
			this.startYear = startYear;
		}

		public int getEndYear() {
			return endYear;
		}

		public void setEndYear(int endYear) {
			this.endYear = endYear;
		}

	}
}
