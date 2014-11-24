package edu.neu.ccs.tagindustrybuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import edu.neu.ccs.constants.Constants;

public class TagIndustryReducer extends Reducer<Text, Text, NullWritable, Text> {

	private StringBuffer buffer;
	private Set<String> industries;
	private Map<String, Integer> skills;
	List<Map.Entry<String, Integer>> skillsList;
	private MultipleOutputs<NullWritable, Text> multipleOutputs;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		buffer = new StringBuffer();
		industries = new HashSet<String>();
		skills = new HashMap<String, Integer>();
		skillsList = new ArrayList<Map.Entry<String, Integer>>();
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		if (key.toString().contains(Constants.UNIQUE_INDUSTRIES_KEY_TAG)) {
			emitTopSkills(key, values, context);
			return;
		}

		buffer.append(key).append(Constants.COMMA);

		for (Text text : values) {
			industries.add(text.toString());
		}

		for (String industry : industries) {

			buffer.append(industry).append(Constants.COMMA);
		}

		// Value contains the tag,value_list as a comma separated string
		multipleOutputs.write("tagindustry", NullWritable.get(), new Text(
				buffer.toString().substring(0, buffer.length() - 1)));

		// Clearing buffer
		buffer.delete(0, buffer.length());

		// CLearing Set
		industries.clear();
	}

	private void emitTopSkills(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		String industry = key.toString().split(Constants.COMMA)[0];

		String skill = null;
		for (Text text : values) {
			skill = text.toString();
			if (skills.containsKey(skill.toString())) {
				skills.put(skill, skills.get(skill) + 1);
			} else {
				skills.put(skill, 1);
			}
		}

		emitTopSkillsHelper(industry, context);
		
		//Clearing skills.
		skills.clear();
	}

	private void emitTopSkillsHelper(String industry, Context context) throws IOException, InterruptedException {
		
		skillsList.addAll(skills.entrySet());

		Collections.sort(skillsList,
				new Comparator<Map.Entry<String, Integer>>() {

					@Override
					public int compare(Entry<String, Integer> entry1,
							Entry<String, Integer> entry2) {

						return -entry1.getValue().compareTo(entry2.getValue());
					}
				});
		
		if (skillsList.size() >= 5) {
			finallyEmit(industry, 5, context);
		}
		else
		{
			finallyEmit(industry, skillsList.size(), context);
		}
		
		//Clearing skills list		
		skillsList.clear();
	}

	private void finallyEmit(String industry, int numberOfSkills, Context context) throws IOException, InterruptedException {

		buffer.append(industry).append(Constants.COMMA);

		for (int i = 0; i < numberOfSkills; i++) {
			buffer.append(skillsList.get(i)).append(Constants.COMMA);

		}

		// Value contains the tag,value_list as a comma separated string
		multipleOutputs.write("topskills", NullWritable.get(), new Text(
				buffer.toString().substring(0, buffer.length() - 1)));

		// Clearing buffer
		buffer.delete(0, buffer.length());
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		multipleOutputs.close();
	}
}