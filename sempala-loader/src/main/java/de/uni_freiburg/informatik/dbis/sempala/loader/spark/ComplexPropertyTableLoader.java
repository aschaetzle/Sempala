package de.uni_freiburg.informatik.dbis.sempala.loader.spark;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import de.uni_freiburg.informatik.dbis.sempala.loader.udf.PropertiesAggregateFunction;

// TODO add class comments - TODO THIS IS NOT A FINAL VERSION OF THE CLASS 
public class ComplexPropertyTableLoader {

	// TODO add comments
	protected static final String tablename_properties = "properties";
	protected static final String tablename_complex_property_table = "complex_property_table";
	protected static final String column_name_is_complex = "is_complex";
	
	/** The location of the input data */
	protected String hdfs_input_directory;
	
	/** The map containing the prefixes */
	public String prefix_file;
	
	/** Indicates if dot at the end of the line is to be stripped */
	public boolean strip_dot;

	/** Indicates if duplicates in the input are to be ignored */
	public boolean unique;

	/** The separator of the fields in the rdf data */
	public String field_terminator = "\\t";

	/** The separator of the lines in the rdf data */
	public String line_terminator = "\\n";

	/*
	 * Triplestore configurations  
	 */
	
	/** The table name of the triple table */
	protected static final String tablename_triple_table = "tripletable";
	
	/** The name used for RDF subject columns */
	public String column_name_subject = "s";

	/** The name used for RDF predicate columns */
	public String column_name_predicate = "p";

	/** The name used for RDF object columns */
	public String column_name_object = "o";
	
	protected static final String table_format_parquet = "parquet";
	
	/** The name of the output table */
	public String tablename_output;

	/** Indicates if shuffle strategy should be used for join operations */
	public boolean shuffle;
	
	/** Indicates if temporary tables must not dropped */
	//TODO this has to be given as a parameter
	public boolean keep = true;

	private Spark connection;

	public ComplexPropertyTableLoader(Spark connection, String hdfsLocation) {
		this.connection = connection;
		this.hdfs_input_directory = hdfsLocation;
	}

	// TODO add comments
	private static String getValidName(String name) {
		return name.replaceAll("[^a-zA-Z0-9_]", "_");
	}

	// TODO add comments
	public static void buildTripleTable(String path, HiveContext hc) {
		hc.sql("CREATE EXTERNAL TABLE triple_table(s STRING, p STRING, o STRING) "
				+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " + "LOCATION '" + path + "'");
	}

	// TODO add comments
	public void savePropertiesIntoTable(HiveContext hiveContext, String tableName) {
		// return rows of format <predicate, is_complex>
		// is_complex can be 1 or 0
		// 1 for multivalued predicate, 0 for single predicate

		// select the properties that are complex
		DataFrame multivaluedProperties = hiveContext.sql(String.format(
				"SELECT DISTINCT(%1$s) AS %1$s FROM (SELECT %2$s, %1$s, COUNT(*) AS rc FROM %3$s GROUP BY %2$s, %1$s HAVING rc > 1) AS grouped",
				column_name_predicate, column_name_subject, tablename_triple_table));


		// select all the properties
		DataFrame allProperties = hiveContext.sql(String.format("SELECT DISTINCT(%1$s) AS %1$s FROM %2$s",
				column_name_predicate, tablename_triple_table));

		// select the properties that are not complex
		DataFrame singledValueProperties = allProperties.except(multivaluedProperties);

		// combine them
		DataFrame combinedProperties = singledValueProperties
				.selectExpr(column_name_predicate, "0 AS " + column_name_is_complex)
				.unionAll(multivaluedProperties.selectExpr(column_name_predicate, "1 AS " + column_name_is_complex));

		// write the result
		combinedProperties.write().mode(SaveMode.Overwrite).saveAsTable(tableName);
	}

	/*
	 * Create the final property table, allProperties contains the list of all
	 * possible properties isComplexProperty contains (in the same order used by
	 * allProperties) the boolean value that indicates if that property is
	 * complex (called also multi valued) or simple.
	 */
	public void buildPropertyTable(HiveContext hc, String[] allProperties, Boolean[] isComplexProperty) {

		// create a new aggregation environment
		PropertiesAggregateFunction aggregator = new PropertiesAggregateFunction(allProperties);

		String predicateObjectColumn = "po";
		String groupColumn = "group";

		// get the compressed table
		DataFrame compressedTriples = hc.sql(String.format("SELECT %s, CONCAT(%s, ' ', %s) AS po FROM %s",
				column_name_subject, column_name_predicate, column_name_object, tablename_triple_table));

		// group by the subject and get all the data
		DataFrame grouped = compressedTriples.groupBy(column_name_subject)
				.agg(aggregator.apply(compressedTriples.col(predicateObjectColumn)).alias(groupColumn));

		// build the query to extract the property from the array
		String[] selectProperties = new String[allProperties.length + 1];
		selectProperties[0] = "s";
		for (int i = 0; i < allProperties.length; i++) {
			// if is not a complex type, extract the value
			String newProperty = isComplexProperty[i]
					? " " + groupColumn + "[" + String.valueOf(i) + "] AS " + getValidName(allProperties[i])
					: " " + groupColumn + "[" + String.valueOf(i) + "][0] AS " + getValidName(allProperties[i]);
			selectProperties[i + 1] = newProperty;
		}

		DataFrame propertyTable = grouped.selectExpr(selectProperties);

		// write the final one
		propertyTable.write().mode(SaveMode.Overwrite).format(table_format_parquet)
				.saveAsTable(tablename_complex_property_table);
	}

	public void load() {

		// table name - properties
		savePropertiesIntoTable(this.connection.getHiveContext(), tablename_properties);

		Row[] props = this.connection.getHiveContext().sql(String.format("SELECT * FROM %s", tablename_properties))
				.collect();
		String[] allProperties = new String[props.length];
		Boolean[] isComplexProperty = new Boolean[props.length];
		for (int i = 0; i < props.length; i++) {
			allProperties[i] = props[i].getString(0);
			isComplexProperty[i] = props[i].getInt(1) == 1;
		}

		buildPropertyTable(this.connection.getHiveContext(), allProperties, isComplexProperty);

		// Drop intermediate tables
		if (!keep) {
			dropTemporaryTables(this.connection.getHiveContext(), tablename_triple_table);
		}
	}

	// drop all the unnecessary tables
	public static void dropTemporaryTables(HiveContext hc, String... tableNames) {
		for (String tb : tableNames)
			hc.sql("DROP TABLE " + tb);
	}

}
