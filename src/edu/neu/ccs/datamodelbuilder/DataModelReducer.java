package edu.neu.ccs.datamodelbuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

import com.google.common.collect.Iterators;
import com.google.gson.Gson;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.objects.Position;
import edu.neu.ccs.objects.Sector;
import edu.neu.ccs.objects.UserProfile;
import edu.neu.ccs.util.UtilHelper;

public class DataModelReducer extends Reducer<Text, UserProfile, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> multipleOutputs;
	private Map<String, List<String>> topTagsPerSector;
	private String topTagsPerSectorFile;
	private Gson gson;
	private FastVector fvWekaAttributes;
	private Map<String, Integer> tagAttribute;
	private Instances trainingSet;
	private int index;
	
	private String fromSector;
	private String toSector;
	private String classLabel;
	private int experience;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		gson = new Gson();
		tagAttribute = new HashMap<String, Integer>();
		
		topTagsPerSectorFile = Constants.TOP_TAGS_SECTOR + System.currentTimeMillis();
		FileSystem.get(context.getConfiguration()).copyToLocalFile(new Path(Constants.TOP_TAGS_SECTOR), new Path(topTagsPerSectorFile));
		UtilHelper.populateKeyValues(topTagsPerSector, topTagsPerSectorFile);
	}

	@Override
	protected void reduce(Text key, Iterable<UserProfile> values, Context context)
			throws IOException, InterruptedException {
		
		// outputs pruned data
		
		if (key.toString().contains(Constants.PRUNED_DATA)) {

			for (UserProfile userProfile : values) {
				multipleOutputs.write(Constants.PRUNED_DATA_TAG,
						NullWritable.get(), new Text(gson.toJson(userProfile)));
			}
			
			return;
		}
		
		String keyValues[] = key.toString().split(Constants.COMMA);
		String year = keyValues[0];
		String sector = keyValues[1];
		
		createModelStructure(sector);
		
		trainingSet = new Instances("traainingSet", fvWekaAttributes, Iterators.size(values.iterator()));
		trainingSet.setClassIndex(index -1);
		
		Instance data = new Instance(index);
		int currentIndex = 0;
		for (UserProfile userprofile : values) {
			
			Set<String> tags = populateTags(userprofile, year);

			if (tags.size() > 0) {
				data.setValue(
						(Attribute) fvWekaAttributes.elementAt(currentIndex),
						Integer.parseInt(userprofile.getNumOfConnections()));
				currentIndex++;

				for (Map.Entry<String, Integer> entry : tagAttribute.entrySet()) {
					if (tags.contains(entry.getKey())) {
						data.setValue((Attribute) fvWekaAttributes
								.elementAt(tagAttribute.get(entry.getKey())),
								"Yes");
					} else {
						data.setValue((Attribute) fvWekaAttributes
								.elementAt(tagAttribute.get(entry.getKey())),
								"No");
					}
					currentIndex++;
				}
				
				data.setValue(
						(Attribute) fvWekaAttributes.elementAt(currentIndex),fromSector);
				currentIndex++;
				
				data.setValue(
						(Attribute) fvWekaAttributes.elementAt(currentIndex),toSector);
				currentIndex++;
				
				data.setValue(
						(Attribute) fvWekaAttributes.elementAt(currentIndex),experience);
				currentIndex++;
				
				data.setValue(
						(Attribute) fvWekaAttributes.elementAt(currentIndex),classLabel);
				currentIndex++;
				
				trainingSet.add(data);
			}	
			
			String classifier = getClassifier();
			
			multipleOutputs.write(Constants.DATA_MODEL_TAG, NullWritable.get(), new Text(new String(classifier)));
			
			tagAttribute.clear();
		}
	}

	private String getClassifier() throws IOException {
		Classifier cModel = (Classifier) new NaiveBayes();
		try {
			cModel.buildClassifier(trainingSet);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return UtilHelper.serialize(cModel);
	}

	private Set<String> populateTags(UserProfile userProfile, String year) {
		
		List<Position> positions = new ArrayList<Position>();
		Set<String> tags = new HashSet<String>();
		Position startingPosition = null;
		
		for(Position position: userProfile.getPositions())
		{
			// TODO this can be done by get(0)
			if (startingPosition == null || 
					Integer.parseInt(position.getStartDate().split(Constants.DATE_SPLITTER)[0]) < 
					Integer.parseInt(startingPosition.getStartDate().split(Constants.DATE_SPLITTER)[0])) {
				startingPosition = position;				
			}
			if (inThisYear(year, position)) {
				positions.add(position);
				tags.add(position.getTitle());
			}
		}
		
		if (positions.size() >= 2) {
			classLabel = "Yes";
			Collections.sort(positions, new Comparator<Position>() {
				@Override
				public int compare(Position position1, Position position2) {
		
					Integer startYear1 = Integer.parseInt(position1.getStartDate().split(Constants.DATE_SPLITTER)[0]);
					Integer startYear2 = Integer.parseInt(position2.getStartDate().split(Constants.DATE_SPLITTER)[0]);
					int cmp = startYear1.compareTo(startYear2);
					
					if (cmp != 0) {
						return cmp;
					}
					
					return cmp;
					/*Integer endYear1 = Integer.parseInt(position1.getEndDate().split(Constants.DATE_SPLITTER)[0]);
					Integer endYear2 = Integer.parseInt(position2.getEndDate().split(Constants.DATE_SPLITTER)[0]);
					return endYear1.compareTo(endYear2);*/
				}
			});
			
			Position firstPosition = positions.get(0);
			Position lastPosition = positions.get(positions.size() -1);
			fromSector = firstPosition.getSector();
			toSector = lastPosition.getSector();
			experience = calculateExperience(startingPosition, year);		
		}
		else if (positions.size() == 1) {
			classLabel = "No";
			Position firstPosition = positions.get(0);
			fromSector = firstPosition.getSector();
			toSector = firstPosition.getSector();
			experience = calculateExperience(startingPosition, year);	
		}

		return tags;
	}

	private int calculateExperience(
			Position firstPosition, 
			String year) {
		int startYear = Integer.parseInt(firstPosition.getStartDate().split(Constants.DATE_SPLITTER)[0]);
		return Integer.parseInt(year) - startYear + 1;
	}

	private boolean inThisYear(String year, Position position) {
		int startYear = Integer.parseInt(position.getStartDate().split(Constants.DATE_SPLITTER)[0]);
		int endYear = 0;
		if (position.getEndDate() != null) {
			endYear = Integer.parseInt(position.getEndDate().split(Constants.DATE_SPLITTER)[0]);
		}
		
		int currentYear = Integer.parseInt(year);
		
		if (startYear <= currentYear && (currentYear <= endYear|| position.isCurrent())) {
			return true;
		}
		return false;
	}

	private void createModelStructure(String sector) {
		List<String> tags = topTagsPerSector.get(sector);

		index = 0;

		Attribute numberOfConnections = new Attribute("numberOfConnections");
		index++;

		List<Attribute> skills = new ArrayList<Attribute>();
		Attribute skill = null;
		FastVector skillVector = null;
		for (String tag : tags) {
			skillVector = new FastVector(2);
			skillVector.addElement("Yes");
			skillVector.addElement("No");
			skill = new Attribute(tag, skillVector);
			skills.add(skill);
			tagAttribute.put(tag, index);
			index++;
		}

		Sector[] sectors = Sector.values();

		FastVector fromSector = new FastVector(sectors.length);
		for (int i = 0; i < sectors.length; i++) {
			fromSector.addElement(sectors[i].name());
		}
		Attribute fromSectorAttribute = new Attribute("fromSector", fromSector);
		index++;

		FastVector toSector = new FastVector(sectors.length);
		for (int i = 0; i < sectors.length; i++) {
			toSector.addElement(sectors[i].name());
		}
		Attribute toSectorAttribute = new Attribute("toSector", toSector);
		index++;
		Attribute experience = new Attribute("experience");
		index++;
		
		FastVector fvClassVal = new FastVector(2);
		fvClassVal.addElement("Yes");
		fvClassVal.addElement("Yes");
		Attribute classAttribute = new Attribute("label", fvClassVal);
		index++;

		fvWekaAttributes = new FastVector(index);
		fvWekaAttributes.addElement(numberOfConnections);
		for (Attribute skillAttr : skills) {
			fvWekaAttributes.addElement(skillAttr);
		}
		fvWekaAttributes.addElement(fromSectorAttribute);
		fvWekaAttributes.addElement(toSectorAttribute);
		fvWekaAttributes.addElement(experience);
		fvWekaAttributes.addElement(classAttribute);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		super.cleanup(context);

		new File(topTagsPerSectorFile).delete();
		
	}
}