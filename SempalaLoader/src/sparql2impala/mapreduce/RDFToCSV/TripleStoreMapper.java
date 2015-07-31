package sparql2impala.mapreduce.RDFToCSV;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import sparql2impala.mapreduce.util.ConfigConstants;
import sparql2impala.mapreduce.util.LineParser;
import sparql2impala.mapreduce.util.ParsedTriple;

/**
 * 
 * @author neua
 * 
 */
public class TripleStoreMapper extends Mapper<LongWritable, Text, Text, Text> {
	boolean isQuads = false;

	LineParser parser;
	HashMap<String, String> prefixes = new HashMap<String, String>();

	protected void setup(Context context) throws IOException {
		parser = new LineParser();
		if (context.getConfiguration().get(ConfigConstants.ISNQUADSKEY)
				.equals(ConfigConstants.ISNQUADSENABLES)) {
			parser = new LineParser(true);
			isQuads = true;
		}

		prefixes.put("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#", "lubm:");
		prefixes.put("http://db.uwaterloo.ca/~galuc/wsdbm/", "wsdbm:");
		prefixes.put("http://dbpedia.org/resource/", "dbp:");
		prefixes.put("http://dbpedia.org/ontology/", "dbpo:" );
		prefixes.put("http://dbpedia.org/property/", "dbpprop:");
		prefixes.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf:");
		prefixes.put("http://www.w3.org/2002/07/owl#", "owl:");
		prefixes.put("http://www.w3.org/2000/01/rdf-schema#", "rdfs:");
		prefixes.put("http://www.w3.org/2001/XMLSchema#", "xsd:");
		prefixes.put("http://schema.org/", "sorg:");
		prefixes.put("http://purl.org/stuff/rev#", "rev:");
		prefixes.put("http://ogp.me/ns#", "og:");
		prefixes.put("http://purl.org/ontology/mo/", "mo:");
		prefixes.put("http://www.geonames.org/ontology#", "gn:");
		prefixes.put("http://purl.org/goodrelations/", "gr:");
		prefixes.put("http://xmlns.com/foaf/", "foaf:");
		prefixes.put("http://purl.org/dc/terms/", "dc:");

	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		ParsedTriple triple;
		try {
			triple = parser.parseLine(value.toString());

			String subject = triple.getSubject();
			String predicate = triple.getPredicate();
			String object = triple.getObject();
			
			
			
			if(context.getConfiguration().get("REPLACE_PREFIXES") != null && context.getConfiguration().get("REPLACE_PREFIXES").equals("TRUE")){
			for (String prefix : prefixes.keySet()) {
				if (subject.contains(prefix))
					subject = replacePrefix(subject, prefix);
				if (predicate.contains(prefix))
					predicate = replacePrefix(predicate, prefix);
				if (object.contains(prefix))
					object = replacePrefix(object, prefix);
			}
			}

			context.write(
					new Text(subject + "\t" + predicate),
					new Text(object));

		} catch (ParseException e) {
			e.printStackTrace();
		}

	}

	public String replacePrefix(String s, String prefix) {
		StringBuilder builder = new StringBuilder(s);
		if(s.contains("<")) builder.deleteCharAt(builder.indexOf("<"));
		if(s.contains(">")) builder.deleteCharAt(builder.indexOf(">"));
		return builder.toString().replace(prefix, prefixes.get(prefix));
	}

}
