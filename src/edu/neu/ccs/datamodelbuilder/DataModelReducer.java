package edu.neu.ccs.datamodelbuilder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.objects.UserProfile;
import edu.neu.ccs.util.UtilHelper;

public class DataModelReducer extends Reducer<Text, UserProfile, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> multipleOutputs;
	private Map<String, List<String>> topTagsPerSector;
	private String topTagsPerSectorFile;
	private StringBuffer buffer;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		buffer = new StringBuffer();
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		
		topTagsPerSectorFile = Constants.TOP_TAGS_SECTOR + System.currentTimeMillis();
		FileSystem.get(context.getConfiguration()).copyToLocalFile(new Path(Constants.TOP_TAGS_SECTOR), new Path(topTagsPerSectorFile));
		UtilHelper.populateKeyValues(topTagsPerSector, topTagsPerSectorFile);
	}

	@Override
	protected void reduce(Text key, Iterable<UserProfile> values, Context context)
			throws IOException, InterruptedException {
		
		for (UserProfile userprofile : values) {
			
			createAndOutputDataModel();
			
			//outputs pruned data
			multipleOutputs.write(Constants.PRUNED_DATA_TAG, NullWritable.get(), userprofile);
		}
	}

	private void createAndOutputDataModel() {
		
		
		
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		super.cleanup(context);

		new File(topTagsPerSectorFile).delete();
		
	}
}