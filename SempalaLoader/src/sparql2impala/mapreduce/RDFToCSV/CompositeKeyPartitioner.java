package sparql2impala.mapreduce.RDFToCSV;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CompositeKeyPartitioner extends Partitioner<CompositeKey, CompositeKey> {

	@Override
	public int getPartition(CompositeKey key, CompositeKey value, int numPartitions) {
		// multiply by 127 to perform some mixing
		return Math.abs(key.getFirst().hashCode() * 127) % numPartitions;
	}
}
