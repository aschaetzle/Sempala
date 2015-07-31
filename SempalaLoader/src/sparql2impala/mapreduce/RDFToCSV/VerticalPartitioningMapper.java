package sparql2impala.mapreduce.RDFToCSV;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import sparql2impala.mapreduce.util.ConfigConstants;
import sparql2impala.mapreduce.util.LineParser;
import sparql2impala.mapreduce.util.ParsedTriple;

/**
 * 
 * @author neua
 * 
 */
public class VerticalPartitioningMapper extends
		Mapper<LongWritable, Text, Text, TextCompositeKey> {
	boolean isQuads = false;

	LineParser parser;
	MultipleOutputs<Text, TextCompositeKey> log;

	protected void setup(Context context) throws IOException {
		parser = new LineParser();
		log = new MultipleOutputs<Text, TextCompositeKey>(context);
		if (context.getConfiguration().get(ConfigConstants.ISNQUADSKEY)
				.equals(ConfigConstants.ISNQUADSENABLES)) {
			parser = new LineParser(true);
			isQuads = true;
		}
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		ParsedTriple triple;
		try {
			triple = parser.parseLine(value.toString());
			if (triple.getSubject() != "") {
				context.write(new Text(triple.getPredicate()),
						new TextCompositeKey(new Text(triple.getSubject()),
								new Text(triple.getObject())));
			} 
		} catch (ParseException e) {
			e.printStackTrace();
		}

	}

}
