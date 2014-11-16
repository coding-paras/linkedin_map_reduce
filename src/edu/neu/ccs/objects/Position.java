package edu.neu.ccs.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparable;

/**
 * Class that holds position information of an employee.
 * 
 * Sample record: { "summary":
 * "Drug Listing for Pharmaceuticals, Annual Report coordinator, specifications and bill of materials."
 * , "title":"Sr. Regulatory Affairs Associate", "start-date":"1978-02-01",
 * "is-current":true, "company-name":"Baxter Healthcare" }
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
	private String sector;

	public Position() {

	}

	public Position(String summary, String title, String companyName,
			boolean isCurrent, String startDate, String endDate, String sector) {

		super();
		this.summary = summary;
		this.title = title;
		this.companyName = companyName;
		this.isCurrent = isCurrent;
		this.startDate = startDate;
		this.endDate = endDate;
		this.sector = sector;
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

	public String getSector() {
		return sector;
	}

	public void setSector(String sector) {
		this.sector = sector;
	}

	public void readFields(DataInput dataInput) throws IOException {
		this.summary = dataInput.readUTF();
		this.title = dataInput.readUTF();
		this.companyName = dataInput.readUTF();
		this.isCurrent = dataInput.readBoolean();
		this.startDate = dataInput.readUTF();
		this.endDate = dataInput.readUTF();
		this.sector = dataInput.readUTF();
	}

	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(summary);
		dataOutput.writeUTF(title);
		dataOutput.writeUTF(companyName);
		dataOutput.writeBoolean(isCurrent);
		dataOutput.writeUTF(startDate);
		dataOutput.writeUTF(endDate);
		dataOutput.writeUTF(sector);

	}

	public int compareTo(Position otherPosition) {
		return new CompareToBuilder()
				.append(this.summary, otherPosition.summary)
				.append(this.title, otherPosition.title)
				.append(this.companyName, otherPosition.companyName)
				.append(this.isCurrent, otherPosition.isCurrent)
				.append(this.startDate, otherPosition.startDate)
				.append(this.endDate, otherPosition.endDate)
				.append(this.sector, otherPosition.sector).toComparison();
	}

	@Override
	public boolean equals(Object otherObject) {
		if (otherObject == null) {
			return false;
		}
		if (otherObject == this) {
			return true;
		}
		if (otherObject.getClass() != getClass()) {
			return false;
		}
		Position otherPosition = (Position) otherObject;
		return new EqualsBuilder().appendSuper(super.equals(otherObject))
				.append(summary, otherPosition.summary)
				.append(title, otherPosition.title)
				.append(companyName, otherPosition.companyName)
				.append(isCurrent, otherPosition.isCurrent)
				.append(startDate, otherPosition.startDate)
				.append(endDate, otherPosition.endDate)
				.append(sector, otherPosition.sector).isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37).append(summary).append(title)
				.append(companyName).append(isCurrent).append(startDate)
				.append(endDate).append(sector).toHashCode();
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("Summary: ").append(summary).append(",")
				.append("Title: ").append(title).append(",")
				.append("CompanyName: ").append(companyName).append(",")
				.append("IsCurrent: ").append(isCurrent).append(",")
				.append("StartDate: ").append(startDate).append(",")
				.append("EndDate: ").append(endDate).append(",")
				.append("Sector: ").append(sector);
		return stringBuilder.toString();
	}

}
