package sparql2impala.mapreduce.RDFToCSV;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import sparql2impala.mapreduce.util.LineParser;
import sparql2impala.mapreduce.util.ParsedTriple;

public class LUBMPreprocessingReducer extends Reducer<Text, Text, Text, Text> {
	HashMap<String, String> prefixes = new HashMap<String, String>();
	LineParser parser = new LineParser();
	ParsedTriple triple = new ParsedTriple();

	protected void setup(Context context) throws IOException {
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

	Text result = new Text();
	final Text emptyText = new Text();
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		try {
			triple = parser.parseLine(key.toString());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		String subject = triple.getSubject();
		String predicate = triple.getPredicate();
		String object = triple.getObject();
		
		for(String prefix : prefixes.keySet()){
			if(subject.contains(prefix))
				subject = replacePrefix(subject, prefix);
			if(predicate.contains(prefix))
				predicate = replacePrefix(predicate, prefix);
			if(object.contains(prefix))
				object = replacePrefix(object, prefix);
		}
		result.set(subject+"\t"+predicate+"\t"+object+" .");
		context.write(result, emptyText);
	}
	
	public String replacePrefix(String s, String prefix){
		StringBuilder builder = new StringBuilder(s);
		builder.deleteCharAt(builder.indexOf("<"));
		builder.deleteCharAt(builder.indexOf(">"));
		return builder.toString().replace(prefix, prefixes.get(prefix));
	}
	

}
