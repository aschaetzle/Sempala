package de.uni_freiburg.informatik.dbis.sempala.translator;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Get the Properties from the DB, with the information about the complex
 * columns. Share them through the singleton pattern.
 * @author Matteo Cossu
 *
 */
public class ComplexProperties {
	private static ComplexProperties singleton;
	private static Map<String, Boolean> isComplexProperty;
	
	 /* A private Constructor prevents any other
	    * class from instantiating.
	    */
	private ComplexProperties() {}

	/*
	 * Get one instance of this class
	 */
	public static ComplexProperties getInstance(Connection connection) {
		if(singleton != null) return singleton;
		
		HashMap<String, Boolean> tempComplexProperties = new HashMap<String, Boolean>();
		
		ResultSet result;
		try {
			result = connection.createStatement().executeQuery(String.format(
					"SELECT * FROM %s;", Tags.COMPLEX_PROPERTIES_TABLENAME));
			while(result.next()){
				String property = result.getString(1);
				Boolean isComplex = result.getBoolean(2);
				tempComplexProperties.put(property, isComplex);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// add also the name of the subject
		tempComplexProperties.put(Tags.SUBJECT_COLUMN_NAME, false);
		
		isComplexProperty = Collections.unmodifiableMap(tempComplexProperties);
		
		return singleton;
	}

	public static Map<String, Boolean> getComplexProperties() {
		return isComplexProperty;
	}
	
	

}
