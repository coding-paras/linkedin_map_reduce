package edu.neu.ccs.datamodelbuilder;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import edu.neu.ccs.constants.Constants;

public class DataModelGroupComparator extends WritableComparator {

	protected DataModelGroupComparator() {
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

		return new CompareToBuilder().append(sector1, sector2).toComparison();
	}
}
