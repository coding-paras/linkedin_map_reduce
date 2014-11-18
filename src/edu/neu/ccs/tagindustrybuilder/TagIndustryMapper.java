package edu.neu.ccs.tagindustrybuilder;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.neu.ccs.objects.Position;
import edu.neu.ccs.objects.UserProfile;

/**
 * Implementation of {@link Mapper} that emits (tag, industry) where tag is a
 * skill of a title. Also, this class increases the global counters for each
 * year.
 */
public class TagIndustryMapper extends Mapper<Object, Text, Text, Text> {

	private static final String DATE_SPLITTER = "-";
	private static final int END_YEAR = 2012;
	private static final String YEAR_COUNTER_GROUP = "YEAR";

	private Gson gson;
	private Type userProfileListType;

	/**
	 * Initializing Gson, setting up type for de-serializing.
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		gson = new Gson();
		userProfileListType = new TypeToken<List<UserProfile>>() {
		}.getType();
	}

	/**
	 * Emits (tag, industry) where tag is a skill of a title. Also, this class
	 * increases the global counters for each year.
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String intermediateString = gson.toJson(value.toString(),
				userProfileListType);

		// De-serializing using Gson
		List<UserProfile> userProfileList = gson.fromJson(intermediateString,
				userProfileListType);

		String industry = null;

		for (UserProfile userProfile : userProfileList) {
			industry = userProfile.getIndustry();
			if (industry != null && !industry.trim().isEmpty()) {
				emitSkillTags(userProfile.getSkillSet(), industry, context);
				emitTitleTags(userProfile.getPositions(), industry, context);
			}
		}
	}

	/**
	 * Emits (title, industry) and increases counter of the years.
	 * 
	 * @param positions
	 * @param industry
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void emitTitleTags(List<Position> positions, String industry,
			Context context) throws IOException, InterruptedException {

		if (positions == null) {
			return;
		}

		String title = null;
		for (Position position : positions) {
			title = position.getTitle();

			if (title != null && !title.trim().isEmpty()) {
				context.write(new Text(title), new Text(industry));
			}

			increaseYearCounters(position, context);
		}
	}

	/**
	 * Increases counter of the years between start date and end date for the
	 * given position.
	 * 
	 * @param position
	 * @param context
	 */
	private void increaseYearCounters(Position position, Context context) {

		String startDate = position.getStartDate();
		String endDate = position.getEndDate();

		if (startDate == null || startDate.trim().isEmpty()) {
			return;
		}

		try {
			int startYear = Integer.parseInt(startDate.split(DATE_SPLITTER)[0]);
			int endYear;
			if (endDate == null || endDate.trim().isEmpty()) {
				endYear = END_YEAR;
			} else {
				endYear = Integer.parseInt(endDate.split(DATE_SPLITTER)[0]);
			}

			for (int i = startYear; i <= endYear; i++) {
				context.getCounter(YEAR_COUNTER_GROUP, String.valueOf(i))
						.increment(1);

			}
		} catch (NumberFormatException numberFormatException) {
			return;
		}
	}

	/**
	 * Emits (skill, industry)
	 * 
	 * @param skillSet
	 * @param industry
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void emitSkillTags(List<String> skillSet, String industry,
			Context context) throws IOException, InterruptedException {

		if (skillSet == null) {
			return;
		}

		for (String skill : skillSet) {
			context.write(new Text(skill), new Text(industry));
		}
	}
}
