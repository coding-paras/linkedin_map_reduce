package edu.neu.ccs.datamodelbuilder;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import edu.neu.ccs.constants.Constants;

public class DataModelKeyComparator extends WritableComparator {

	protected DataModelKeyComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable writableComparable1,
			WritableComparable writableComparable2) {
		Text yearSector1 = (Text) writableComparable1;
		Text yearSector2 = (Text) writableComparable2;

		if (yearSector1.toString().contains(Constants.PRUNED_DATA) || 
				yearSector2.toString().contains(Constants.PRUNED_DATA)) {

			return yearSector1.compareTo(yearSector2);
		}

		String sector1 = yearSector1.toString().split(Constants.COMMA)[1];
		String sector2 = yearSector2.toString().split(Constants.COMMA)[1];

		Integer year1 = Integer.parseInt(yearSector1.toString().split(Constants.COMMA)[0]);
		Integer year2 = Integer.parseInt(yearSector2.toString().split(Constants.COMMA)[0]);

		int cmp = new CompareToBuilder().append(year1, year2).toComparison();

		if (cmp != 0) {
			return cmp;
		}
		return new CompareToBuilder().append(sector1, sector2).toComparison();

	}
}
