package edu.neu.ccs.datamodelbuilder;

import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.objects.UserProfile;

public class DataModelPartitioner extends Partitioner<Text, UserProfile> {

	private Random random;
	public DataModelPartitioner() {
		
		this.random = new Random();
	}

	
	@Override
    public int getPartition(Text key, UserProfile value, int numReduceTasks) {
		
		String[] values = key.toString().split(Constants.COMMA);
		
		if (values[0].contains(Constants.PRUNED_DATA)) {
			
			return random.nextInt(numReduceTasks);
		} else {
			
			return Integer.parseInt(values[0]) % numReduceTasks;
						
		}
	}
}
