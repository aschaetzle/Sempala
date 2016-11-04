package de.uni_freiburg.informatik.dbis.sempala.translator.sql;

import java.util.HashMap;
import java.util.Map;

public class Schema {

	public static Map<String, String[]> shiftToParent(
			Map<String, String[]> original, String table) {
		HashMap<String, String[]> result = new HashMap<String, String[]>();
		for (String key : original.keySet()) {
			result.put(key, new String[] { table, key });

		}
		return result;
	}

	public static Map<String, String[]> shiftOrigin(
			Map<String, String[]> original, String table) {
		HashMap<String, String[]> result = new HashMap<String, String[]>();
		for (String key : original.keySet()) {
			String[] entry = original.get(key);
			if (entry.length == 2)
				result.put(key, new String[] { table, entry[1] });
			else
				result.put(key, new String[] { table, entry[0] });

		}
		return result;
	}

}
