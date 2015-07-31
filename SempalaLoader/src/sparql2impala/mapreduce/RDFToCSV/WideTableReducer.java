package sparql2impala.mapreduce.RDFToCSV;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import sparql2impala.mapreduce.util.Crossproduct;
import sparql2impala.mapreduce.util.Index;

public class WideTableReducer extends
		Reducer<CompositeKey, CompositeKey, Text, Text> {

	MultipleOutputs<Text, Text> mos;
	final Text emptyText = new Text();

	TreeSet<String> predicates = new TreeSet<String>();;

	boolean ignoreType = false;
	String datasetName = "";

	protected void setup(Context context) throws IOException {
		mos = new MultipleOutputs<Text, Text>(context);

		Configuration conf = context.getConfiguration();
		if (conf.get("IS_PARTITIONED_BY_TYPE") != null) {
			ignoreType = conf.get("IS_PARTITIONED_BY_TYPE").equals("TRUE");
		}
		this.datasetName = conf.get("datasetName");

		try {
			//Path path = new Path("/user/neua/" + datasetName + "_preprocessing/");
			Path path = new Path(datasetName + "_preprocessing/");
			FileSystem fs = FileSystem.get(new Configuration());
			FileUtil.copyMerge(FileSystem.get(conf), path,
					FileSystem.getLocal(conf).getRaw(), new Path("tempfile"),
					false, conf, "");

			fs = FileSystem.getLocal(conf).getRaw();

			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(new Path("tempfile"))));
			String line = br.readLine();
			while (line != null) {
				predicates.add(line.split("\\s+")[0].trim());
				line = br.readLine();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

	Text output = new Text();
	ArrayList<ArrayList<HashMap<String, String>>> listofListOfRows = new ArrayList<ArrayList<HashMap<String, String>>>();;
	ArrayList<HashMap<String, String>> list;
	String prev;
	HashMap<String, String> map;
	ArrayList<HashMap<String, String>> results;
	String keyAsString = "";

	public void reduce(CompositeKey key, Iterable<CompositeKey> values,
			Context context) throws IOException, InterruptedException {
		// in hope of solving memory issues
		listofListOfRows = new ArrayList<ArrayList<HashMap<String, String>>>();
		prev = "";
		list = new ArrayList<HashMap<String, String>>();
		boolean first = true;
		keyAsString = key.getFirst().toString();

		for (CompositeKey value : values) {
			if (!(value.getFirst().toString().equals(prev))) {
				if (first) {
					first = false;
				} else {
					listofListOfRows.add(list);
				}
				list = new ArrayList<HashMap<String, String>>();
			}

			map = new HashMap<String, String>();
			// map.put(value.getFirst().toString(),
			// value.getSecond().toString());
			map.put(value.getFirst().toString(), value.getSecond().toString());
			list.add(map);
			prev = value.getFirst().toString();
		}
		// last value?
		listofListOfRows.add(list);

		writeCrossProductRowByRow(listofListOfRows, context, keyAsString);
	}

	public void writeCrossProductRowByRow(
			ArrayList<ArrayList<HashMap<String, String>>> lists,
			Context context, String first) throws IOException,
			InterruptedException {

		int[] maxValues = new int[lists.size()];
		// initialize max values
		for (int i = 0; i < lists.size(); i++) {
			maxValues[i] = lists.get(i).size();
		}
		Index index = new Index(maxValues);

		// iterate over arrays similar to n-nary counting
		// e.g 4 lists, indexes: 0 0 0 0, then 0 0 0 1, 0 0 0 2, 0 0 1 0
		boolean cont = true;
		while (cont) {
			// do stuff
			HashMap<String, String> row = new HashMap<String, String>();
			for (int i = 0; i < index.getIndexValue().length; i++) {
				row.putAll(lists.get(i).get(index.getIndexValue()[i]));
			}
			writeResult(row, context, first);
			cont = index.increment();
		}

	}

	StringBuilder path;

	public void writeResult(HashMap<String, String> result, Context context,
			String key) throws IOException, InterruptedException {
		output.set(makeRow(key, result));

		if (!ignoreType) {
			context.write(output, emptyText);
		} else {
			if (result.containsKey("rdf:type")) {
				//path = new StringBuilder("/user/neua/");
				path = new StringBuilder("");
				path.append(datasetName);
				path.append("/");
				path.append(result.get("rdf:type").replace(':', '_'));
				path.append("/tabledata");
			} else {
				//path = new StringBuilder("/user/neua/");
				path = new StringBuilder("");
				path.append(datasetName);
				path.append("/");
				path.append("NULL");
				path.append("/tabledata");
			}
			mos.write(output, emptyText, path.toString());
		}
	}

	StringBuilder builder;

	private String makeRow(String key, HashMap<String, String> row) {
		builder = new StringBuilder();
		builder.append(key);
		builder.append("\t");
		for (String predicate : this.predicates) {
			if (!(ignoreType && predicate.equals("rdf:type"))) {
				if (row.containsKey(predicate)) {
					builder.append(row.get(predicate));
				} else {
					builder.append("\\N");
				}
				builder.append("\t");
			}
		}
		return builder.toString();
	}

}
