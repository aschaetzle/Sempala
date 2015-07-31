package com.antony_neu.thesis.RDFToCSV;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.antony_neu.thesis.util.ConfigConstants;
import com.antony_neu.thesis.util.LineParser;
import com.antony_neu.thesis.util.ParsedTriple;

/**
 * 
 * @author neua
 * 
 */
public class WideTableMapper extends
		Mapper<LongWritable, Text, CompositeKey, CompositeKey> {
	boolean isQuads = false;
	final Text one = new Text("1");

	LineParser parser;
	// MultipleOutputs<CompositeKey, CompositeKey> log;
	HashMap<String, Boolean> histogram = new HashMap<String, Boolean>();

	protected void setup(Context context) throws IOException {
		parser = new LineParser();
		// log = new MultipleOutputs<CompositeKey, CompositeKey>(context);
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
				// multivalue predicate?
				if (!histogram.containsKey(triple.getPredicate()) || histogram.get(triple.getPredicate())) {
					context.write(
							new CompositeKey(triple.getPredicate(), triple
									.getSubject()),
							new CompositeKey(triple.getSubject(), triple
									.getObject()));
				} else {
					// not multivalue
					context.write(
							new CompositeKey("WIDETABLE", triple.getSubject()),
							new CompositeKey(triple.getPredicate(), triple
									.getObject()));
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
		}

	}

}
