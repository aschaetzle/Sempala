package de.uni_freiburg.informatik.dbis.sempala.translator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Row;

import de.uni_freiburg.informatik.dbis.sempala.translator.spark.Spark;

/**
 * Get the columns' names of a complex property table from the DB, with the
 * information about their type. for more information see
 * {@link ComplexPropertyTableColumns#getColumns()}. Share them through the
 * singleton pattern.
 * 
 * @author Matteo Cossu, Polina Koleva
 *
 */
public class ComplexPropertyTableColumns {

	private static ComplexPropertyTableColumns singleton;
	// name of the column and its type (true for complex, false for simple)
	private static Map<String, Boolean> columns;

	/*
	 * A private Constructor prevents any other class from instantiating.
	 */
	private ComplexPropertyTableColumns() {
	}

	/*
	 * Get the only instance of this class. Initialize its properties using an
	 * impala connection.
	 */
	public static ComplexPropertyTableColumns getInstance(Connection connection) {
		if (singleton != null)
			return singleton;

		HashMap<String, Boolean> tempColumns = new HashMap<String, Boolean>();

		ResultSet result;
		try {
			result = connection.createStatement()
					.executeQuery(String.format("SELECT * FROM %s;", Tags.COMPLEX_PROPERTIES_TABLENAME));
			while (result.next()) {
				String property = result.getString(1);
				Boolean isComplex = result.getBoolean(2);
				tempColumns.put(property, isComplex);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		// add also the name of the subject
		tempColumns.put(Tags.SUBJECT_COLUMN_NAME, false);
		columns = Collections.unmodifiableMap(tempColumns);
		return singleton;
	}

	/*
	 * Get the only instance of this class. Initialize its properties using a
	 * Spark connection.
	 */
	public static ComplexPropertyTableColumns getInstance(Spark connection) {
		if (singleton != null) {
			return singleton;
		}
		HashMap<String, Boolean> tempColumns = new HashMap<String, Boolean>();
	
		Row[] props = connection.sql(String.format("SELECT * FROM %s", Tags.COMPLEX_PROPERTIES_TABLENAME)).collect();
		for (int i = 0; i < props.length; i++) {
			tempColumns.put(props[i].getString(0), props[i].getInt(1) == 1);
		}

		// add also the name of the subject
		tempColumns.put(Tags.SUBJECT_COLUMN_NAME, false);
		columns = Collections.unmodifiableMap(tempColumns);
		return singleton;
	}

	/**
	 * Get all columns from the complex property table and its corresponding
	 * type. If a column is of simple type (string, integer and so on) its value
	 * in the map is false. Otherwise is a column is of complex type (array, map
	 * and so on), its corresponding key in the map is true.
	 */
	public static Map<String, Boolean> getColumns() {
		return columns;
	}

}
