package edu.neu.ccs.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Sample record:
 * {
 * 	"summary":"Drug Listing for Pharmaceuticals, Annual Report coordinator, specifications and bill of materials.",
 * 	"title":"Sr. Regulatory Affairs Associate",
 * 	"start-date":"1978-02-01",
 * 	"is-current":true,
 * 	"company-name":"Baxter Healthcare"
 * }
 * 
 *
 */
public class Position implements WritableComparable<Position> {

	private String summary;
	private String title;
	private String companyName;
	private boolean isCurrent;
	private String startDate;
	private String endDate;
	
	public Position () {
		
	}
	
	public Position(String summary, String title, String companyName, boolean isCurrent, String startDate, String endDate) {
		
		super();
		this.summary = summary;
		this.title = title;
		this.companyName = companyName;
		this.isCurrent = isCurrent;
		this.startDate = startDate;
		this.endDate = endDate;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getCompanyName() {
		return companyName;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}

	public boolean isCurrent() {
		return isCurrent;
	}

	public void setCurrent(boolean isCurrent) {
		this.isCurrent = isCurrent;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public int compareTo(Position other) {
		// TODO Auto-generated method stub
		return 0;
	}

}
