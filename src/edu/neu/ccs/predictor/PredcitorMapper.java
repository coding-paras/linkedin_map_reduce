package edu.neu.ccs.predictor;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.neu.ccs.objects.UserProfile;

public class PredcitorMapper extends Mapper<Object, Text, Text, UserProfile> {

	private Gson gson;
	private Type userProfileListType;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		gson = new Gson();
		userProfileListType = new TypeToken<List<UserProfile>>() {
		}.getType();
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		List<UserProfile> userProfileList = gson.fromJson(value.toString(),
				userProfileListType);
		for (UserProfile userProfile : userProfileList) {
			context.write(new Text(userProfile.getIndustry()), userProfile);
		}
	}
}
