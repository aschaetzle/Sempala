package sparql2impala.mapreduce.RDFToCSV;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import sparql2impala.mapreduce.util.ConfigConstants;

public class VerticalPartitioningReducer extends
		Reducer<Text, TextCompositeKey, Text, Text> {
	MultipleOutputs<Text, Text> mos;
	boolean logging;

	protected void setup(Context context) throws IOException {
		mos = new MultipleOutputs<Text, Text>(context);
		logging = context.getConfiguration().get(ConfigConstants.loggingKey)
				.equals(ConfigConstants.loggingEnabled);
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

	Text mosValue = new Text();
	Text reduceKey = new Text();
	Text reduceValue = new Text();
	Text value;

	public void reduce(Text key, Iterable<TextCompositeKey> values,
			Context context) throws IOException, InterruptedException {

		for (TextCompositeKey value : values) {
			mos.write(value.getFirst(), value.getSecond(), "tables/"
					+ "table"+key.toString().replace(':', '_') + "/tabledata");
		}
		mos.write(new Text("table"+key.toString().replace(':', '_')), key, "index/index");

	}

}
