package sparql2impala.mapreduce.RDFToCSV;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import sparql2impala.mapreduce.util.ConfigConstants;

public class PreprocessingReducer extends Reducer<Text, Text, Text, Text> {
	MultipleOutputs<Text, Text> mos;
	boolean logging;
	final Text undefinedText = new Text("UNDEFINED");

	String datasetName = "";

	protected void setup(Context context) throws IOException {
		mos = new MultipleOutputs<Text, Text>(context);
		logging = context.getConfiguration().get(ConfigConstants.loggingKey)
				.equals(ConfigConstants.loggingEnabled);

	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String type = values.iterator().next().toString();
		if (!type.equals("")) {
			context.write(key, new Text(type));
		} else {
			context.write(key, undefinedText);
		}
	}

}
