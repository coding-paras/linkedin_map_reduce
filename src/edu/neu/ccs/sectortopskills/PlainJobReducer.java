package edu.neu.ccs.sectortopskills;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.objects.UserProfile;

public class PlainJobReducer extends Reducer<Text, UserProfile, NullWritable, Text> {

	private StringBuffer buffer;
	private Map<String, String> cityCountry;
	private Map<String, Integer> countryCount;
	private String countryCityCSV;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		buffer = new StringBuffer();
		cityCountry = new HashMap<String, String>();
		countryCount = new HashMap<String, Integer>();
		
		// Reading country city csv
		countryCityCSV = Constants.COUNTRY_CITY_CSV + System.currentTimeMillis();
		Path topTagsPerSectorPath = new Path(countryCityCSV);
		FileSystem.get(context.getConfiguration()).copyToLocalFile(new Path(Constants.COUNTRY_CITY_CSV), topTagsPerSectorPath);
		
		populateCityCountry();
	}

	private void populateCityCountry() throws IOException {
		
		BufferedReader bufferedReader =  new BufferedReader(new FileReader(countryCityCSV));
		
		String record = null;
		String recordValues[] = null;
		
		String country = null;
		String state = null;
		String city = null;
		
		while((record = bufferedReader.readLine()) != null)
		{
			recordValues = record.split(Constants.COMMA);
			country = recordValues[0];
			state = recordValues[1];
			city = recordValues[2];
			
			cityCountry.put(country, country);
			cityCountry.put(state, country);
			cityCountry.put(city, country);
		}
		
		bufferedReader.close();
		
	}

	@Override
	protected void reduce(Text key, Iterable<UserProfile> values, Context context)
			throws IOException, InterruptedException {
		
		String country = null;
		Integer count = null;
		for (UserProfile userProfile : values) {
			country = getCountry(userProfile);
			if (country == null) {
				continue;
			}
			count = countryCount.get(country);
			if (count == null) {
				count = 0;
			}
			countryCount.put(country, count + 1);
		}
		
		String sector = key.toString();
		
		for (Map.Entry<String, Integer> entry : countryCount.entrySet()) {
			buffer.append(sector).append(Constants.COMMA).append(entry.getKey()).append(Constants.COMMA).append(entry.getValue());
			context.write(NullWritable.get(), new Text(buffer.toString()));
			buffer.delete(0, buffer.length());
		}
		
		countryCount.clear();
	}

	private String getCountry(UserProfile userProfile) {
		String locationTags[] = userProfile.getLocation().split(Constants.SPACE);
		String country = null;
		for (String locationTag : locationTags) {
			country = cityCountry.get(locationTag);
			if (country != null) {
				return country;
			}
		}
		return null;
	}
	
	@Override
	protected void cleanup(Context context)throws IOException, InterruptedException {
		
		File countryCityCSVFile = new File(countryCityCSV);		
		if (countryCityCSVFile.exists()) {
			countryCityCSVFile.delete();
		}
	}
}