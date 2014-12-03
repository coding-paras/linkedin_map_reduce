package edu.neu.ccs.datamodelbuilder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.constants.Constants.ClassLabel;
import edu.neu.ccs.objects.Position;
import edu.neu.ccs.objects.Sector;
import edu.neu.ccs.objects.UserProfile;
import edu.neu.ccs.util.UtilHelper;

public class DataModelReducer extends Reducer<Text, Text, NullWritable, Text> {

	private static Logger logger = Logger.getLogger(DataModelReducer.class);
	
	private MultipleOutputs<NullWritable, Text> multipleOutputs;
	private Map<String, List<String>> topTagsPerSector;
	private String topTagsPerSectorFile;
	private Gson gson;
	private Type userProfileType;
	private FastVector wekaAttributes;
	private Map<String, Integer> tagAttributeMap;
	private Instances trainingSet;
	private int index;
	
	//data model attributes
	private ClassLabel classLabel;
	
	private List<Classifier> classfiers;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		gson = new Gson();
		userProfileType = new TypeToken<UserProfile>() {}.getType();
		tagAttributeMap = new HashMap<String, Integer>();
		
		topTagsPerSectorFile = Constants.TOP_TAGS_SECTOR + System.currentTimeMillis();
		FileSystem.get(context.getConfiguration()).copyToLocalFile(new Path(Constants.TOP_TAGS_SECTOR), new Path(topTagsPerSectorFile));
		topTagsPerSector = UtilHelper.populateKeyValues(topTagsPerSectorFile);
		classfiers = new ArrayList<Classifier>();
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// outputs pruned data
		if (key.toString().contains(Constants.PRUNED_DATA)) {

			for (Text value : values) {

				multipleOutputs.write(Constants.PRUNED_DATA_TAG,
						NullWritable.get(), value);
			}
			return;
		}

		String sector = key.toString().split(Constants.COMMA)[1];

		if (sector == null || sector.trim().isEmpty() || sector.equals("null")) {

			context.getCounter("DATAMODEL", "NULL-SECTOR").increment(1);
			// TODO - change the logic?
			return;
		}

		createModelStructure(sector);

		try {

			String previousYear = key.toString().split(Constants.COMMA)[0];
			String currentYear = previousYear;

			List<UserProfile> userProfiles = new ArrayList<UserProfile>();
			Classifier classifier = null;
			for (Text value : values) {
				
				currentYear = key.toString().split(Constants.COMMA)[0];
				UserProfile userProfile = (UserProfile) gson.fromJson(value.toString(), userProfileType);
				if (!currentYear.equals(previousYear)) {
					
					classifier = machineLearn(userProfiles, context, previousYear, sector);
					if (classifier != null) {
						classfiers.add(classifier);
					}
					userProfiles.clear();
					userProfiles.add(userProfile);
					previousYear = currentYear;
				} 
				else {
					userProfiles.add(userProfile);
				}
			}

			classifier = machineLearn(userProfiles, context, previousYear, sector);
			if (classifier != null) {
				classfiers.add(classifier);
			}
			
			serializeClassifiers(sector, context);
			
			//reset
			classfiers.clear();
			tagAttributeMap.clear();
		} catch (Exception e) {
			
			logger.error(e);
		}

	}

	private void serializeClassifiers(String sector, Context context) {
		
		Classifier[] classifierObjects = new Classifier[classfiers.size()];		
		
		for (int i = 0; i < classifierObjects.length; i++) {
			classifierObjects[i] = classfiers.get(i);			
		}

		try {
			// outputs the DataModel
			SerializationHelper.writeAll("/tmp/" + sector,classifierObjects);

			Configuration conf = context.getConfiguration();
			FileSystem.get(conf).copyFromLocalFile(
					new Path("/tmp/" + sector),
					new Path(conf.get(Constants.SECOND_OUTPUT_FOLDER) + File.separator + sector));
			new File("/tmp/" + sector).delete();

		} catch (Exception e) {
			
			logger.error(e);
		}
		
	}

	private Classifier machineLearn(List<UserProfile> userProfiles, Context context, String year, String sector) throws Exception {

		if (year.equals(context.getConfiguration().get(Constants.TEST_YEAR,"2012"))) {
			
			emitTestData(userProfiles);
			return null;
		}
		
		trainingSet = new Instances("trainingSet", wekaAttributes, userProfiles.size());
		trainingSet.setClassIndex(index - 1);

		Instance data = new Instance(index);
		for (UserProfile userProfile : userProfiles) {

			int currentIndex = 0;
			Set<String> tags = populateTagsAndSetClassifier(userProfile, year);

			if (userProfile.getPositions().size() > 0) {

				data.setValue((Attribute) wekaAttributes.elementAt(currentIndex),
						Integer.parseInt(userProfile.getNumOfConnections()));
				currentIndex++;

				for (Entry<String, Integer> entry : tagAttributeMap.entrySet()) {
					
					if (tags.contains(entry.getKey())) {
						
						data.setValue((Attribute) wekaAttributes.elementAt(tagAttributeMap.get(entry.getKey())), 
								ClassLabel.YES.toString());
					} else {
						
						data.setValue((Attribute) wekaAttributes.elementAt(tagAttributeMap.get(entry.getKey())), 
								ClassLabel.NO.toString());
					}
					currentIndex++;
				}

				data.setValue((Attribute) wekaAttributes.elementAt(currentIndex),
						sector);
				currentIndex++;

				data.setValue((Attribute) wekaAttributes.elementAt(currentIndex),
						userProfile.getRelevantExperience());
				currentIndex++;

				data.setValue((Attribute) wekaAttributes.elementAt(currentIndex),
						classLabel.toString());
				currentIndex++;

				trainingSet.add(data);
			}
		}
		
		return getClassifier();
	}

	private void emitTestData(List<UserProfile> userProfiles) throws IOException, InterruptedException {
		
		for (UserProfile userProfile : userProfiles) {
			// outputs the test data
			multipleOutputs.write(Constants.TEST_DATA_TAG, NullWritable.get(),
					new Text(gson.toJson(userProfile)));
		}
	}

	//private String getClassifier() throws Exception {
	private Classifier getClassifier() throws Exception {
		
		Classifier cModel = (Classifier) new NaiveBayes();
		cModel.buildClassifier(trainingSet);
		//return UtilHelper.serialize(cModel);
		return cModel;
	}

	private Set<String> populateTagsAndSetClassifier(UserProfile userProfile, String year) {
		
		List<Position> positions = userProfile.getPositions();
		Set<String> tags = new HashSet<String>();
		
		for(Position position: positions) {

			tags.add(position.getTitle());
		}
		tags.addAll(userProfile.getSkillSet());
		
		if (positions.size() >= 2) {
			
			classLabel = ClassLabel.YES;
		}
		else if (positions.size() == 1) {
			
			classLabel = ClassLabel.NO;
		}

		return tags;
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
			skillVector.addElement(ClassLabel.YES.toString());
			skillVector.addElement(ClassLabel.NO.toString());
			skill = new Attribute(tag, skillVector);
			skills.add(skill);
			tagAttributeMap.put(tag, index);
			index++;
		}

		Sector[] sectors = Sector.values();

		FastVector sectorVector = new FastVector(sectors.length);
		for (int i = 0; i < sectors.length; i++) {
			
			sectorVector.addElement(sectors[i].name());
		}
		Attribute sectorAttribute = new Attribute("sector", sectorVector);
		index++;
		
		Attribute experience = new Attribute("experience");
		index++;
		
		FastVector classVariable = new FastVector(2);
		classVariable.addElement(ClassLabel.YES.toString());
		classVariable.addElement(ClassLabel.NO.toString());
		Attribute classAttribute = new Attribute("label", classVariable);
		index++;

		wekaAttributes = new FastVector(index);
		wekaAttributes.addElement(numberOfConnections);
		for (Attribute skillAttr : skills) {
			
			wekaAttributes.addElement(skillAttr);
		}
		wekaAttributes.addElement(sectorAttribute);
		wekaAttributes.addElement(experience);
		wekaAttributes.addElement(classAttribute);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		super.cleanup(context);

		new File(topTagsPerSectorFile).delete();
		
	}
}