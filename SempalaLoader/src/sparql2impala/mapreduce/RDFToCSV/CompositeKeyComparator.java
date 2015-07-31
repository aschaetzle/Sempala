package sparql2impala.mapreduce.RDFToCSV;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
	protected CompositeKeyComparator() {
		super(CompositeKey.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKey ip1 = (CompositeKey) w1;
		CompositeKey ip2 = (CompositeKey) w2;
		return ip1.compareTo(ip2);
	}
}