package edu.neu.ccs.predictor;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.constants.Constants.ClassLabel;
import edu.neu.ccs.objects.ConfusionMatrix;
import edu.neu.ccs.objects.Position;
import edu.neu.ccs.objects.Sector;
import edu.neu.ccs.objects.UserProfile;
import edu.neu.ccs.util.UtilHelper;

public class PredictorReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	private Gson gson;
	private Type userProfileType;

	private static Logger logger = Logger.getLogger(PredictorReducer.class);
	
	private StringBuffer buffer;
	
	private int actualLeft;
	private int predictLeft;
	
	private static String module;

	private List<Classifier> sectorDataModels;
	private Map<String, List<String>> topTagsPerSector;
	private String topTagsPerSectorFile;
	
	private String dataModelsFile;

	private FastVector wekaAttributes;
	private int index;
	private Map<String, Integer> tagAttribute;
	private Instances testingSet;

	private ClassLabel classLabel;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);
		
		gson = new Gson();
		userProfileType = new TypeToken<UserProfile>() {}.getType();
		
		buffer = new StringBuffer();
		actualLeft = 0;
		predictLeft = 0;
		
		module = context.getConfiguration().get(Constants.MODULE, "PREDICTOR");
		
		this.sectorDataModels = new ArrayList<Classifier>();
		
		// Reading top tags file.
		topTagsPerSectorFile = Constants.TOP_TAGS_SECTOR + System.currentTimeMillis();
		Path topTagsPerSectorPath = new Path(topTagsPerSectorFile);
		FileSystem.get(context.getConfiguration()).copyToLocalFile(new Path(Constants.TOP_TAGS_SECTOR), topTagsPerSectorPath);
		topTagsPerSector = UtilHelper.populateKeyValues(topTagsPerSectorPath.toString());
		
		tagAttribute = new HashMap<String, Integer>();
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		if (key == null || key.toString().equals("null")) {
			
			context.getCounter(module, Constants.NULL_SECTOR).increment(1);
			//TODO - change the logic?
			return;
		}
		
		try {
			
			populateSectorDataModels(key.toString(), context.getConfiguration());
		} 
		catch (Exception e) {
			
			//TODO - log the error
			throw new RuntimeException("Error occurred while populating the data model");
		}
		
		createModelStructure(key.toString());
		UserProfile parsedUserProfile = null;
		List<UserProfile> userProfiles = new ArrayList<UserProfile>();
		for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();) {
			parsedUserProfile = gson.fromJson(iterator.next().toString(), userProfileType);
			userProfiles.add(parsedUserProfile);
		}
		testingSet = new Instances("testingSet", wekaAttributes, userProfiles.size());
		testingSet.setClassIndex(index - 1);
		

		Instance data = new Instance(index);
		
		for (UserProfile userProfile : userProfiles) {
			int currentIndex = 0;

			Set<String> tags = populateTags(userProfile);

			if (userProfile.getPositions().size() > 0) {
				
				data.setValue((Attribute) wekaAttributes.elementAt(currentIndex),
						Integer.parseInt(userProfile.getNumOfConnections()));
				currentIndex++;

				for (Map.Entry<String, Integer> entry : tagAttribute.entrySet()) {
					
					if (tags.contains(entry.getKey())) {
						
						data.setValue((Attribute) wekaAttributes.elementAt(tagAttribute.get(entry.getKey())),
								ClassLabel.YES.toString());
					} 
					else {
						
						data.setValue((Attribute) wekaAttributes.elementAt(tagAttribute.get(entry.getKey())),
								ClassLabel.NO.toString());
					}
					currentIndex++;
				}
				
				data.setValue((Attribute) wekaAttributes.elementAt(currentIndex), key.toString());
				currentIndex++;

				data.setValue((Attribute) wekaAttributes.elementAt(currentIndex), userProfile.getRelevantExperience());
				currentIndex++;

				data.setValue((Attribute) wekaAttributes.elementAt(currentIndex),classLabel.toString());
				currentIndex++;
				
				testingSet.add(data);
			}
		}

		tagAttribute.clear();

		try {
			
			predict(key.toString(), context);
		} 
		catch (Exception e) {
			
			logger.error(e);
		}
		
		emitStats(context, key.toString(), userProfiles.size());
		
		buffer.delete(0, buffer.length());
		
		actualLeft = 0;
		predictLeft = 0;
		this.sectorDataModels.clear();
		
		new File(dataModelsFile).delete();
	}
	
	private void emitStats(Context context, String sector, int testDataSize) throws IOException, InterruptedException {
		buffer.append(sector).append(Constants.COMMA)
		.append(testDataSize).append(Constants.COMMA)
		.append(actualLeft).append(Constants.COMMA)
		.append(predictLeft);
		
		context.write(NullWritable.get(), new Text(buffer.toString()));
		
	}

	private void populateSectorDataModels(String sector, Configuration conf) throws Exception {
		
		// Reading sector models
		dataModelsFile = Constants.MODELS + sector + System.currentTimeMillis();
		Path dataModelsPath = new Path(dataModelsFile);
		FileSystem.get(conf).copyToLocalFile(new Path(Constants.MODELS + sector), dataModelsPath);
		extractDataModels(dataModelsPath);
	}
	
	private void extractDataModels(Path sectorFilePath) throws Exception {
		
		Object[] sectorClassifiers = SerializationHelper.readAll(sectorFilePath.toString());
		
		for (Object object : sectorClassifiers) {
			this.sectorDataModels.add((Classifier)object);
		}
	}

	@SuppressWarnings("unchecked")
	private void predict(String sector, Context context) throws Exception {
		
		Enumeration<Instance> enumeration = testingSet.enumerateInstances();
		double actualValue, predictedValue;
		while (enumeration.hasMoreElements()) {
			Instance instance = enumeration.nextElement();
			actualValue = instance.classValue();
			predictedValue = predictHelper(instance, sectorDataModels);
			
			if (actualValue == 0.0 && predictedValue == 0.0) {
				// TN
				context.getCounter(ConfusionMatrix.TRUE_NEGATIVE).increment(1);
			}
			else if (actualValue == 0.0 && predictedValue == 1.0) {
				// FP
				predictLeft ++;
				context.getCounter(ConfusionMatrix.FALSE_POSITIVE).increment(1);
			}
			else if (actualValue == 1.0 && predictedValue == 0.0) {
				//FN
				actualLeft ++;
				context.getCounter(ConfusionMatrix.FALSE_NEGATIVE).increment(1);
			}
			else if (actualValue == 1.0 && predictedValue == 1.0) {
				// TP
				predictLeft++;
				actualLeft++;
				context.getCounter(ConfusionMatrix.TRUE_POSITIVE).increment(1);
			}
		}
	}

	private double predictHelper(Instance instance, List<Classifier> sectorModels) throws Exception {
		int finalVote = 0;
		for (Classifier classifier : sectorModels) {
			
			if (classifier.classifyInstance(instance) == 1.0) {
				
				finalVote ++;
			}
			else {
				
				finalVote --;
			}
		}
		return finalVote > 0 ? 1 :0;
	}

	private Set<String> populateTags(UserProfile userProfile) {
		
		List<Position> positions = userProfile.getPositions();
		
		Set<String> tags = new HashSet<String>();
		
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
			tagAttribute.put(tag, index);
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