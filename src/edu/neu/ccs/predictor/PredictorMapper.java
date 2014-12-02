package edu.neu.ccs.predictor;

import java.io.IOException;
import java.lang.reflect.Type;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import edu.neu.ccs.objects.UserProfile;

public class PredictorMapper extends Mapper<Object, Text, Text, UserProfile> {

	private Gson gson;
	private Type userProfileType;

	private static Logger logger = Logger.getLogger(PredictorMapper.class);

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		gson = new Gson();
		userProfileType = new TypeToken<UserProfile>() {}.getType();
	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		try {
			
			UserProfile userProfile = gson.fromJson(value.toString(), userProfileType);

			context.write(new Text(userProfile.getIndustry()), userProfile);
			
		} catch (JsonSyntaxException jse) {
			logger.error(jse);
			return;
		}

	}
}
