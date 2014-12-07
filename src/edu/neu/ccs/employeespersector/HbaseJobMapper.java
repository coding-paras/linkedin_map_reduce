package edu.neu.ccs.employeespersector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import edu.neu.ccs.constants.Constants;
import edu.neu.ccs.constants.Constants.UserProfileEnum;
import edu.neu.ccs.objects.Position;
import edu.neu.ccs.objects.UserProfile;

public class HbaseJobMapper extends TableMapper<Text, UserProfile> {

	/**
	 * Initializing Gson, setting up type for de-serializing.
	 */
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
	}

	protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

		// Extracting values
		String firstName = new String(value.getValue(Constants.COLUMN_FAMILY_BYTES,
				Bytes.toBytes(UserProfileEnum.FIRSTNAME.name())));
		String lastName = new String(value.getValue(Constants.COLUMN_FAMILY_BYTES,
				Bytes.toBytes(UserProfileEnum.LASTNAME.name())));
		String location = new String(value.getValue(Constants.COLUMN_FAMILY_BYTES,
				Bytes.toBytes(UserProfileEnum.LOCATION.name())));
		String numOfConnections = new String(value.getValue(Constants.COLUMN_FAMILY_BYTES,
				Bytes.toBytes(UserProfileEnum.NUMCONNECTIONS.name())));
		String lastPositionCompany = new String(value.getValue(Constants.COLUMN_FAMILY_BYTES,
				Bytes.toBytes(UserProfileEnum.POSITION_LAST_KNOWN_COMPANY.name())));
		String lastPositionTitle = new String(value.getValue(Constants.COLUMN_FAMILY_BYTES,
				Bytes.toBytes(UserProfileEnum.POSITION_LAST_KNOWN_TITLE.name())));

		// Extracting the start date and the sector
		String[] attribs = new String(key.get()).split(Constants.COMMA);

		List<Position> positions = new ArrayList<Position>();
		Position position = new Position(Constants.EMPTY_STRING, lastPositionTitle, lastPositionCompany, false, attribs[0], Constants.EMPTY_STRING, attribs[1]);
		positions.add(position);

		UserProfile userProfile = new UserProfile(firstName, lastName, numOfConnections, attribs[1], location, new ArrayList<String>(), positions);

		if (attribs[1].equals("null") || attribs[1].isEmpty() || location.isEmpty()) {

			return;
		}

		context.write(new Text(userProfile.getIndustry()), userProfile);
	}
}
