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
 * Creates distinct triples during precprocessing.
 * @author neua
 * 
 */
public class LUBMPreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {
	final Text emptyText = new Text();


	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(value, emptyText);

	}

}
