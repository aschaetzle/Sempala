package sparql2impala.sql;

import sparql2impala.Tags;

public class NameFilter {

	public static String filter(String s){
		if(Tags.restrictedNames.containsKey(s)){
			System.out.println("Found illegal name "+s);
			return Tags.restrictedNames.get(s);
		} else {
			return s;
		}
	}
	
	
}
