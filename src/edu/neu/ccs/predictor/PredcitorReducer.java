package edu.neu.ccs.predictor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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

import com.google.common.collect.Iterators;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.constants.Constants.ClassLabel;
import edu.neu.ccs.objects.ConfusionMatrix;
import edu.neu.ccs.objects.Position;
import edu.neu.ccs.objects.Sector;
import edu.neu.ccs.objects.UserProfile;
import edu.neu.ccs.util.UtilHelper;

public class PredcitorReducer extends
		Reducer<Text, UserProfile, NullWritable, Text> {

	private static Logger logger = Logger.getLogger(PredcitorReducer.class);

	private Map<String, List<Classifier>> models;
	private Map<String, List<String>> topTagsPerSector;

	private FastVector wekaAttributes;
	private int index;
	private Map<String, Integer> tagAttribute;
	private Instances testingSet;


	private ClassLabel classLabel;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);

		String modelsFile = Constants.MODELS + System.currentTimeMillis();
		FileSystem.get(context.getConfiguration()).copyToLocalFile(new Path(Constants.MODELS), new Path(modelsFile));

		try {
			models = new HashMap<String, List<Classifier>>();
			populateModels(modelsFile);
		} catch (ClassNotFoundException e) {
			logger.error(e);
		}
		
		topTagsPerSector = populateTagsFromCache(context.getConfiguration());
		tagAttribute = new HashMap<String, Integer>();
	}

	private Map<String, List<String>> populateTagsFromCache(Configuration configuration) throws IOException {
		
		Map<String, List<String>> topTagsPerSector = new HashMap<String, List<String>>();

		Path[] localFiles = DistributedCache.getLocalCacheFiles(configuration);

		// TODO Same as job runner.
		String topTagsFile = configuration.get(Constants.TOP_TAGS);
		topTagsFile = topTagsFile.substring(topTagsFile.lastIndexOf("/") + 1);

		if (localFiles == null) {

			throw new RuntimeException("DistributedCache not present in HDFS");
		}

		for (Path path : localFiles) {

			if (topTagsFile.equals(path.getName())) {				
				topTagsPerSector = UtilHelper.populateKeyValues(path.toString());
			}
		}

		return topTagsPerSector;
	}

	private void populateModels(String modelsFile) throws IOException,
			ClassNotFoundException {

		BufferedReader bufferedReader = new BufferedReader(new FileReader(
				new File(modelsFile)));

		String line = null;
		String values[] = null;

		String sector = null;
		String modelString = null;

		Classifier model = null;

		List<Classifier> sectorModles = null;
		while ((line = bufferedReader.readLine()) != null) {
			values = line.split(Constants.COMMA);
			sector = values[1];
			modelString = values[2];

			model = (Classifier) UtilHelper.deserialize(modelString);

			sectorModles = models.get(sector);

			if (sectorModles == null) {
				sectorModles = new ArrayList<Classifier>();
				models.put(sector, sectorModles);
			}
			sectorModles.add(model);
		}
		bufferedReader.close();
	}

	@Override
	protected void reduce(Text key, Iterable<UserProfile> values,
			Context context) throws IOException, InterruptedException {

		createModelStructure(key.toString());

		testingSet = new Instances("testingSet", wekaAttributes,
				Iterators.size(values.iterator()));
		testingSet.setClassIndex(index - 1);

		Instance data = new Instance(index);
		for (UserProfile userprofile : values) {
			
			int currentIndex = 0;

			Set<String> tags = populateTags(userprofile, "2012");

			if (tags.size() > 0) {
				data.setValue(
						(Attribute) wekaAttributes.elementAt(currentIndex),
						Integer.parseInt(userprofile.getNumOfConnections()));
				currentIndex++;

				for (Map.Entry<String, Integer> entry : tagAttribute.entrySet()) {
					if (tags.contains(entry.getKey())) {
						data.setValue((Attribute) wekaAttributes
								.elementAt(tagAttribute.get(entry.getKey())),
								ClassLabel.YES.toString());
					} else {
						data.setValue((Attribute) wekaAttributes
								.elementAt(tagAttribute.get(entry.getKey())),
								ClassLabel.NO.toString());
					}
					currentIndex++;
				}
				
				data.setValue((Attribute) wekaAttributes.elementAt(currentIndex), key.toString());

				currentIndex++;

				data.setValue((Attribute) wekaAttributes.elementAt(currentIndex), userprofile.getRelevantExperience());
				currentIndex++;

				data.setValue((Attribute) wekaAttributes.elementAt(currentIndex),classLabel.toString());
				currentIndex++;
				
				
				testingSet.add(data);
			}
		}

		tagAttribute.clear();

		try {
			predict(key.toString(), context);
		} catch (Exception e) {
			logger.error(e);
		}

	}

	@SuppressWarnings("unchecked")
	private void predict(String sector, Context context) throws Exception {
		
		List<Classifier> sectorModels = models.get(sector);
		Enumeration<Instance> enumeration = testingSet.enumerateInstances();
		double actualValue, predictedValue;
		while (enumeration.hasMoreElements()) {
			Instance instance = enumeration.nextElement();
			actualValue = instance.classValue();
			predictedValue = predictHelper(instance, sectorModels);
			
			if (actualValue == 0.0 && predictedValue == 0.0) {
				// TN
				context.getCounter(ConfusionMatrix.TRUE_NEGATIVE).increment(1);
			}

			else if (actualValue == 0.0 && predictedValue == 1.0) {
				// FP
				context.getCounter(ConfusionMatrix.FALSE_POSITIVE).increment(1);
			}

			else if (actualValue == 1.0 && predictedValue == 0.0) {
				//FN
				context.getCounter(ConfusionMatrix.FALSE_NEGATIVE).increment(1);
			}

			else if (actualValue == 1.0 && predictedValue == 1.0) {
				// TP
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
			else
			{
				finalVote --;
			}
		}
		return finalVote > 0 ? 1 :0;
	}

	private Set<String> populateTags(UserProfile userProfile, String year) {
		
		List<Position> positions = new ArrayList<Position>();
		Set<String> tags = new HashSet<String>();
		
		for(Position position: userProfile.getPositions()) {

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
			skillVector.addElement(ClassLabel.YES);
			skillVector.addElement(ClassLabel.NO);
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
		classVariable.addElement(ClassLabel.YES);
		classVariable.addElement(ClassLabel.NO);
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
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	}
}