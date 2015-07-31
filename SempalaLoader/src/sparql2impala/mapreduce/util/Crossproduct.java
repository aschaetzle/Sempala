package sparql2impala.mapreduce.util;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Calculates the crossproduct of two lists of rows. The rows are represented as
 * HashMaps where the key is the column name und value is the column value
 * 
 * @author neua
 * 
 */

public class Crossproduct {
	// assume keys are not equal
	private static ArrayList<HashMap<String, String>> result;
	private static HashMap<String, String> row;

	public static ArrayList<HashMap<String, String>> crossProduct(
			ArrayList<HashMap<String, String>> x,
			ArrayList<HashMap<String, String>> y) {
		result = new ArrayList<HashMap<String, String>>();
		for (HashMap<String, String> rowX : x) {
			for (HashMap<String, String> rowY : y) {
				row = new HashMap<String, String>();
				for (String key : rowX.keySet()) {
					row.put(key, rowX.get(key));
				}
				for (String key : rowY.keySet()) {
					row.put(key, rowY.get(key));
				}
				result.add(row);
			}
		}

		return result;
	}


}
