package edu.neu.ccs.constants;

public class Constants {
	
	public static final String COMMA = ",";
	public static final String DATE_SPLITTER = "-";
	public static final int END_YEAR = 2012;
	public static final String INDUSTRY_SECTOR_FILE = "industry_sector_file";
	public static final int START_YEAR = 1980;
	public static final String TAG_INDUSTRY_FILE = "/tmp/tag_industries.txt";
	public static final String TOP_TAGS_SECTOR = "/tmp/top_tags_sector.txt";
	public static final String UNIQUE_INDUSTRIES_KEY_TAG = "#I#";
	public static final String YEAR_COUNTER_GRP = "YEAR";
	public static final String PRUNED_DATA = "#PD#";
	
	// JOB 1 output names
	public static final String TAG_INDUSTRY = "tagindustry";
	public static final String TOP_TAGS = "toptags";
	
	public enum ClassLabel {
		
		YES("1"), NO("0");
		
		private String value;
		
		private ClassLabel(String value) {
			
			this.value = value;
		}
		
		@Override
		public String toString() {
			
			return this.value;
		}
	}
	public static final String TEST_YEAR = "testyear";
	// JOB 2 output names
	public static final String DATA_MODEL_TAG = "datamodel";
	public static final String PRUNED_DATA_TAG = "pruneddata";
	public static final String TEST_DATA_TAG = "testdata";
	public static final String TOP_TAGS_FILE_TAG = "toptagsfile";
}
