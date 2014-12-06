package edu.neu.ccs.sectortopskills.hbase.load;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.constants.Constants.UserProfileEnum;
import edu.neu.ccs.objects.Position;
import edu.neu.ccs.objects.UserProfile;

public class HbaseLoadMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
	
	private Gson gson;
	private Type userProfileType;
	private HTable table;
	
	private static Logger logger = Logger.getLogger(HbaseLoadMapper.class);

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		// Opening the Linkedin table
		Configuration conf = context.getConfiguration();
		table = new HTable(conf, conf.get(TableOutputFormat.OUTPUT_TABLE));
		
		super.setup(context);
	}
	
	@Override
	protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException {

		UserProfile userProfile = null;
		try {

			userProfile = gson.fromJson(value.toString(), userProfileType);
		} 
		catch (JsonSyntaxException jse) {
			
			logger.error(jse);
			return;
		}

		int finalStartYear = Integer.MAX_VALUE, finalEndYear = Integer.MIN_VALUE;
		int startYear, endYear;
		String keyStr = null;
		ImmutableBytesWritable rowKey = null;
		
		String sector = userProfile.getIndustry();
		Map<String, Position> lastKnownPositionPerYearSector = new HashMap<String, Position>();
		for (Position position : userProfile.getPositions()) {

			if (position.getStartDate() == null) {
				
				continue;
			}
			
			startYear = Integer.parseInt(position.getStartDate().split(Constants.DATE_DELIMITER_1)[0]);
			startYear = (startYear < Constants.START_YEAR ? Constants.START_YEAR : startYear);
			endYear = (position.getEndDate() == null ? Constants.END_YEAR : 
				Integer.parseInt(position.getEndDate().split(Constants.DATE_DELIMITER_1)[0]));
			
			if (startYear < finalStartYear) {
				
				finalStartYear = startYear;
			}
			
			if (endYear > finalEndYear) {
				
				finalEndYear = endYear;
			}
			
			for (int i = startYear; i <= endYear; i++) {
				
				keyStr = i + Constants.COMMA + sector;
				lastKnownPositionPerYearSector.put(keyStr, position);
			}
		}
		
		Position lastKnownPosition = null;
		Put put = null;
		for (int i = finalStartYear; i <= finalEndYear; i++) {
			
			// Emitting user profile for each year
			keyStr = i + Constants.COMMA + sector + Constants.COMMA + System.currentTimeMillis();
			rowKey = new ImmutableBytesWritable(Bytes.toBytes(keyStr));
			
			//Creating a new row
	        put = new Put(rowKey.get());
	        
	        //Adding the firstname of the user
	        put.add(Constants.COLUMN_FAMILY_BYTES, Bytes.toBytes(UserProfileEnum.FIRSTNAME.name()), Bytes.toBytes(userProfile.getFirstName()));
	        
	        //
	        put.add(Constants.COLUMN_FAMILY_BYTES, Bytes.toBytes(UserProfileEnum.LASTNAME.name()), Bytes.toBytes(userProfile.getLastName()));

	        //
	        put.add(Constants.COLUMN_FAMILY_BYTES, Bytes.toBytes(UserProfileEnum.LOCATION.name()), Bytes.toBytes(userProfile.getLocation()));
	        
	        //
	        put.add(Constants.COLUMN_FAMILY_BYTES, Bytes.toBytes(UserProfileEnum.NUMCONNECTIONS.name()), Bytes.toBytes(userProfile.getNumOfConnections()));
	        
	        //
	        lastKnownPosition = lastKnownPositionPerYearSector.get(i + Constants.COMMA + sector);
	        
	        //
	        put.add(Constants.COLUMN_FAMILY_BYTES, Bytes.toBytes(UserProfileEnum.POSITION_LAST_KNOWN_COMPANY.name()), Bytes.toBytes(lastKnownPosition == null ? null : lastKnownPosition.getCompanyName()));
	        
	        //
	        put.add(Constants.COLUMN_FAMILY_BYTES, Bytes.toBytes(UserProfileEnum.POSITION_LAST_KNOWN_TITLE.name()), Bytes.toBytes(lastKnownPosition == null ? null : lastKnownPosition.getTitle()));
	        
	        //
	        put.add(Constants.COLUMN_FAMILY_BYTES, Bytes.toBytes(UserProfileEnum.REL_EXPERIENCE.name()), Bytes.toBytes(userProfile.getRelevantExperience()));
	        
	        //Writing the record to the table.
	        context.write(rowKey, put);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,	InterruptedException {

		super.cleanup(context);
		
		// Close the open table
		table.close();
	}
}
