package edu.neu.ccs.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparable;

import com.google.gson.annotations.SerializedName;

import edu.neu.ccs.constants.Constants;

/**
 * Sample record: 
 * {
 * 	"skills":["Regulatory Affairs","Regulatory Requirements","FDA","Regulatory Intelligence","Pharmaceutical Industry","NDA"],
 * 	"positions":[
 * 		{
 * 			"summary":"Drug Listing for Pharmaceuticals, Annual Report coordinator, specifications and bill of materials.",
 * 			"title":"Sr. Regulatory Affairs Associate",
 * 			"start-date":"1978-02-01",
 * 			"is-current":true,
 * 			"company-name":"Baxter Healthcare"
 * 		}],
 * 	"public-profile-url":"/pub/vos-l/33/b91/754",
 * 	"location":"Greater Chicago Area",
 * 	"first-name":"Vos",
 * 	"num-connections":"2",
 * 	"last-name":"L",
 * 	"industry":"Pharmaceuticals"
 * }
 * 
 */
public class UserProfile implements WritableComparable<UserProfile> {
	
	@SerializedName(value="skills")
	private List<String> skillSet = new ArrayList<String>();
	private List<Position> positions = new ArrayList<Position>();
	private String location;
	@SerializedName(value="num-connections")
	private String numOfConnections;
	@SerializedName(value="first-name")
	private String firstName;
	@SerializedName(value="last-name")
	private String lastName;
	private String industry;
	private Integer relevantExperience;
	
	public UserProfile() {
		
	}
	
	public UserProfile(String firstName, String lastName, String numOfConnections,String industry, String location, List<String> skillSet,
			List<Position> positions) {
		
		super();
		this.firstName = firstName;
		this.lastName = lastName;
		this.numOfConnections = numOfConnections;
		this.industry = industry;
		this.location = location;
		this.skillSet = (skillSet != null ? skillSet : this.skillSet);
		this.positions = (positions != null ? positions : this.positions);
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getNumOfConnections() {
		return numOfConnections;
	}

	public void setNumOfConnections(String numOfConnections) {
		this.numOfConnections = numOfConnections;
	}

	public String getIndustry() {
		return industry;
	}

	public void setIndustry(String industry) {
		this.industry = industry;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public List<String> getSkillSet() {
		return skillSet;
	}

	public void setSkillSet(List<String> skillSet) {
		this.skillSet = skillSet;
	}

	public List<Position> getPositions() {
		return positions;
	}

	public void setPositions(List<Position> positions) {
		this.positions = positions;
	}

	public Integer getRelevantExperience() {
		return relevantExperience;
	}

	public void setRelevantExperience(Integer relevantExperience) {
		this.relevantExperience = relevantExperience;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.firstName = in.readUTF();
		this.lastName = in.readUTF();
		this.numOfConnections = in.readUTF();
		this.industry = in.readUTF();
		this.location = in.readUTF();
		int count = in.readInt(); //skill count
		skillSet.clear();
		for (int i = 0; i < count; i++) {

			skillSet.add(in.readUTF());
		}
		positions.clear();
		count = in.readInt(); //Position count
		Position position = null;
		for (int i = 0; i < count; i++) {

			position = new Position();
			position.readFields(in);
			positions.add(position);
		}
		this.relevantExperience = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(this.firstName != null ? this.firstName : Constants.EMPTY_STRING);
		out.writeUTF(this.lastName != null ? this.lastName : Constants.EMPTY_STRING);
		out.writeUTF(this.numOfConnections != null ? this.numOfConnections : "0");
		out.writeUTF(this.industry != null ? this.industry : Constants.EMPTY_STRING);
		out.writeUTF(this.location != null ? this.location : Constants.EMPTY_STRING);
		out.writeInt(this.skillSet.size()); //skill count
		for(String skill : this.skillSet) {
			
			out.writeUTF(skill);
		}
		out.writeInt(this.positions.size()); //position count
		for (Position position : this.positions) {

			position.write(out);
		}
		out.writeInt(this.relevantExperience != null ? this.relevantExperience : 0);
	}

	@Override
	public int compareTo(UserProfile o) {
		
		UserProfile that = (UserProfile) o;
		return new CompareToBuilder()
		.append(this.firstName, that.firstName)
		.append(this.lastName, that.lastName)
		.append(this.numOfConnections, that.numOfConnections)
		.append(this.industry, that.industry)
		.append(this.location, that.location)
		.append(this.skillSet, that.skillSet)
		.append(this.positions, that.positions)
		.append(this.relevantExperience, that.relevantExperience)
		.toComparison();
	}

	@Override
	public boolean equals(Object obj) {
		
		if (obj == null || obj.getClass() != getClass()) {
			
			return false;
		}
		if (obj == this) { return true; }
		UserProfile that = (UserProfile) obj;
		return new EqualsBuilder()
		.appendSuper(super.equals(obj))
		.append(this.firstName, that.firstName)
		.append(this.lastName, that.lastName)
		.append(this.numOfConnections, that.numOfConnections)
		.append(this.industry, that.industry)
		.append(this.location, that.location)
		.append(this.skillSet, that.skillSet)
		.append(this.positions, that.positions)
		.append(this.relevantExperience, that.relevantExperience)
		.isEquals();
	}
	
	@Override
	public int hashCode() {
		
		return new HashCodeBuilder(17, 37)
		.append(this.firstName)
		.append(this.lastName)
		.append(this.numOfConnections)
		.append(this.industry)
		.append(this.location)
		.append(this.skillSet)
		.append(this.positions)
		.append(this.relevantExperience)
		.toHashCode();
	}
	
	@Override
	public String toString() {
		
		StringBuilder stringBuilder = new StringBuilder();
		
		stringBuilder.append(this.getClass().getName()).append(":").append("{");
		stringBuilder.append("First Name: ").append(this.firstName).append(", ");
		stringBuilder.append("Last Name: ").append(this.lastName).append(", ");
		stringBuilder.append("Number of Connections: ").append(this.numOfConnections).append(", ");
		stringBuilder.append("Industry: ").append(this.industry).append(", ");
		stringBuilder.append("Location: ").append(this.location).append(", ");
		stringBuilder.append("Skills Set: ").append("[").append(this.skillSet).append("]").append(", ");
		stringBuilder.append("Positions: ").append("[").append(this.positions).append("]").append("}");
		stringBuilder.append("Relevant Experience: ").append("[").append(this.relevantExperience).append("]").append("}");
		
		return stringBuilder.toString();
	}
}