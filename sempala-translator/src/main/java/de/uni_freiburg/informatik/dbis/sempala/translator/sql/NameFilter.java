package de.uni_freiburg.informatik.dbis.sempala.translator.sql;

import de.uni_freiburg.informatik.dbis.sempala.translator.Tags;

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
