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
public class PreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {
	boolean isQuads = false;
	final Text emptyText = new Text();

	LineParser parser;
	MultipleOutputs<Text, TextCompositeKey> log;

	protected void setup(Context context) throws IOException {
		parser = new LineParser();
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
				if (triple.isObjectTypedLiteral()) {
					context.write(new Text(triple.getPredicate()), new Text(triple.getObjectLiteralType()));
				} else {
					context.write(new Text(triple.getPredicate()), emptyText);
				}
			} else {
				// TODO
				// log.write(new Text("error"),
				// new Text("input was" + value.toString()), "errorLog");
				// log.write(new Text("error"), new Text("triple was" + triple),
				// "errorLog");

			}
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (NullPointerException ne){
			ne.printStackTrace();
		}

	}

}
