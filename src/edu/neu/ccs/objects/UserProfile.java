package edu.neu.ccs.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

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

	private String firstName;
	private String lastName;
	private int numOfConnections;
	private String industry;
	private String location;
	private List<String> skillSet = new ArrayList<String>();
	private List<Position> positions = new ArrayList<Position>();
	
	public UserProfile() {
		
	}
	
	public UserProfile(String firstName, String lastName, int numOfConnections,String industry, String location, List<String> skillSet,
			List<Position> positions) {
		
		super();
		this.firstName = firstName;
		this.lastName = lastName;
		this.numOfConnections = numOfConnections;
		this.industry = industry;
		this.location = location;
		this.skillSet = skillSet;
		this.positions = positions;
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

	public int getNumOfConnections() {
		return numOfConnections;
	}

	public void setNumOfConnections(int numOfConnections) {
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

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public int compareTo(UserProfile o) {
		// TODO Auto-generated method stub
		return 0;
	}


}
