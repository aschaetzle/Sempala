package de.uni_freiburg.informatik.dbis.sempala.translator.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JoinUtil {

	public static ArrayList<String> getSharedVars(
			Map<String, String[]> leftSchema, Map<String, String[]> rightSchema) {
		ArrayList<String> result = new ArrayList<String>();
		for (String key : rightSchema.keySet()) {
			if (leftSchema.containsKey(key))
				result.add(key);
		}
		return result;
	}

	public static List<String> getOnConditions(Map<String, String[]> leftSchema,
			Map<String, String[]> rightSchema) {
		ArrayList<String> conditions = new ArrayList<String>();
		ArrayList<String> sharedVars = getSharedVars(leftSchema, rightSchema);
		if (sharedVars.size() == 0) {
			System.err.println("Warning! Found cross product!");
			return new ArrayList<String>();
		} else {

			for (String var : sharedVars) {
				conditions.add(leftSchema.get(var)[0] + "." + leftSchema.get(var)[1]  + "="
						+ rightSchema.get(var)[0] + "." + rightSchema.get(var)[1]);
			}
		}
		return conditions;
	}
	
	public static String generateConjunction(List<String> conditions){
		String result = "";
		
		boolean first = true;
		for (String cond : conditions) {
			if (!first) {
				result += " AND ";
			} else {
				first = false;
			}
			result += cond;

		}
		return result;
	}

}
