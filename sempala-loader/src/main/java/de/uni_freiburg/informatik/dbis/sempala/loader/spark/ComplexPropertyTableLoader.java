package de.uni_freiburg.informatik.dbis.sempala.loader.spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.functions;

import de.uni_freiburg.informatik.dbis.sempala.loader.udf.PropertiesAggregateFunction;

/**
 * Class that construct complex property table. It operates over set of rdf
 * triples, collects and transforms information about them into a table. If we have
 * a list of predicates/properties p1, ... , pN, then the scheme of the table is
 * (s: STRING, p1: LIST<STRING> OR STRING, ..., pN: LIST<STRING> OR STRING).
 * Column s contains subjects. For each subject , there is only one row in the
 * table. Each predicate can be of complex or simple type. If a predicate is of
 * simple type means that there is no subject which has more than one triple
 * containing this property/predicate. Then the predicate column is of type
 * STRING. Otherwise, if a predicate is of complex type which means that there
 * exists at least one subject which has more than one triple containing this
 * property/predicate. Then the predicate column is of type LIST<STRING>.
 * 
 * @author Polina Koleva, Matteo Cossu
 *
 */
public class ComplexPropertyTableLoader {

	/** The location of the input data. */
	protected String hdfs_input_directory;

	/** The file containing the prefixes. */
	public String prefix_file;

	/** A map containing the prefixes **/
	private Map<String, String> prefix_map;

	/** Indicates if dot at the end of the line is to be stripped. */
	public boolean strip_dot;

	/** Indicates if duplicates in the input are to be ignored. */
	public boolean unique;

	/** The separator of the fields in the rdf data. */
	public String field_terminator = "\\t";

	/** The separator of the lines in the rdf data. */
	public String line_terminator = "\\n";

	/*
	 * Triplestore configurations
	 */

	/**
	 * The table name of the table that contains all rdf triples. The scheme of
	 * the table is (s:STRING, p:STRING, o:OBJECT). All prefixes if a file with
	 * prefixes is given are replaced in the table.
	 */
	protected static final String tablename_triple_table = "tripletable";

	/**
	 * The name of the table that stores all properties/predicates and their
	 * type - are they simple or complex. The scheme of the table is (p :
	 * STRING, is_complex : BOOLEAN). There is a row for each predicate.
	 */
	protected static final String tablename_properties = "properties";

	/**
	 * The name of the table that stores the transformed triple table into a
	 * complex property table (the result of this loader). If we have a list of
	 * predicates/properties p1, ... , pN, then the scheme of the table is (s:
	 * STRING, p1: LIST<STRING> OR STRING, ..., pN: LIST<STRING> OR STRING).
	 * Column s contains subjects. For each subject , there is only one row in
	 * the table. Each predicate can be of complex or simple type. If a
	 * predicate is of simple type means that there is no subject which has more
	 * than one triple containing this property/predicate. Then the predicate
	 * column is of type STRING. Otherwise, if a predicate is of complex type
	 * which means that there exists at least one subject which has more than
	 * one triple containing this property/predicate. Then the predicate column
	 * is of type LIST<STRING>.
	 */
	protected static final String tablename_complex_property_table = "complex_property_table";

	/** The name used for RDF subject columns. */
	public String column_name_subject = "s";

	/** The name used for RDF predicate columns. */
	public String column_name_predicate = "p";

	/** The name used for RDF object columns. */
	public String column_name_object = "o";
	
	/** Separator used to distinguish two values in the same string  */
	public String columns_separator = "\\$%";

	/**
	 * The name used for an properties table column which indicates if a
	 * property is simple or complex.
	 */
	protected static final String column_name_is_complex = "is_complex";

	/**
	 * The format in which the tables in Spark are stored especially the result
	 * table of this loader.
	 */
	protected static final String table_format_parquet = "parquet";

	/** Indicates if temporary tables must not dropped. */
	public boolean keep;

	/** Spark connection used for executing queries. */
	private Spark connection;

	/** Hive context of a Spark connection. */
	private HiveContext hiveContext;

	public ComplexPropertyTableLoader(Spark connection, String hdfsLocation) {
		this.connection = connection;
		this.hiveContext = connection.getHiveContext();
		this.hdfs_input_directory = hdfsLocation;
		this.strip_dot = false;
	}

	/**
	 * Build a table that contains all rdf triples. If a file with prefixes is
	 * given, they will be replaced. See
	 * {@link ComplexPropertyTableLoader#tablename_triple_table}.
	 */
	public void buildTripleTable() {

		String createTripleTable = String.format(
				"CREATE EXTERNAL TABLE %s(%s STRING, %s STRING, %s STRING) ROW FORMAT DELIMITED"
						+ " FIELDS TERMINATED BY '%s'  LINES TERMINATED BY '%s' LOCATION '%s'",
						tablename_triple_table, column_name_subject, column_name_predicate, column_name_object,
				field_terminator, line_terminator, hdfs_input_directory);

		this.hiveContext.sql(createTripleTable);

	}

	/**
	 * Create "properties" table. See:
	 * {@link ComplexPropertyTableLoader#tablename_properties}.
	 */
	public void savePropertiesIntoTable() {
		// return rows of format <predicate, is_complex>
		// is_complex can be 1 or 0
		// 1 for multivalued predicate, 0 for single predicate

		// select the properties that are complex
		DataFrame multivaluedProperties = this.hiveContext.sql(String.format(
				"SELECT DISTINCT(%1$s) AS %1$s FROM (SELECT %2$s, %1$s, COUNT(*) AS rc FROM %3$s GROUP BY %2$s, %1$s HAVING rc > 1) AS grouped",
				column_name_predicate, column_name_subject, tablename_triple_table));

		// select all the properties
		DataFrame allProperties = this.hiveContext.sql(String.format("SELECT DISTINCT(%1$s) AS %1$s FROM %2$s",
				column_name_predicate, tablename_triple_table));

		// select the properties that are not complex
		DataFrame singledValueProperties = allProperties.except(multivaluedProperties);

		// combine them
		DataFrame combinedProperties = singledValueProperties
				.selectExpr(column_name_predicate, "0 AS " + column_name_is_complex)
				.unionAll(multivaluedProperties.selectExpr(column_name_predicate, "1 AS " + column_name_is_complex));
		
		// remove '<' and '>', convert the characters
		DataFrame cleanedProperties = combinedProperties.withColumn("p", functions.regexp_replace(functions.translate(combinedProperties.col("p"), "<>", ""), 
				"[[^\\w]+]", "_"));
		
		// write the result
		cleanedProperties.write().mode(SaveMode.Overwrite).saveAsTable(tablename_properties);
	}

	/**
	 * Create the final property table, allProperties contains the list of all
	 * possible properties isComplexProperty contains (in the same order used by
	 * allProperties) the boolean value that indicates if that property is
	 * complex (called also multi valued) or simple.
	 */
	public void buildComplexPropertyTable(String[] allProperties, Boolean[] isComplexProperty) {

		// create a new aggregation environment
		PropertiesAggregateFunction aggregator = new PropertiesAggregateFunction(allProperties, columns_separator);

		String predicateObjectColumn = "po";
		String groupColumn = "group";

		// get the compressed table
		DataFrame compressedTriples = this.hiveContext.sql(String.format("SELECT %s, CONCAT(%s, '%s', %s) AS po FROM %s",
				column_name_subject, column_name_predicate, columns_separator, column_name_object, tablename_triple_table));

		// group by the subject and get all the data
		DataFrame grouped = compressedTriples.groupBy(column_name_subject)
				.agg(aggregator.apply(compressedTriples.col(predicateObjectColumn)).alias(groupColumn));

		// build the query to extract the property from the array
		String[] selectProperties = new String[allProperties.length + 1];
		selectProperties[0] = column_name_subject;
		for (int i = 0; i < allProperties.length; i++) {
			
			// if property is a full URI, remove the < at the beginning end > at the end
			String rawProperty = allProperties[i].startsWith("<") && allProperties[i].endsWith(">") ? 
					allProperties[i].substring(1, allProperties[i].length() - 1) :  allProperties[i];
			// if is not a complex type, extract the value
			String newProperty = isComplexProperty[i]
					? " " + groupColumn + "[" + String.valueOf(i) + "] AS " + getValidColumnName(rawProperty)
					: " " + groupColumn + "[" + String.valueOf(i) + "][0] AS " + getValidColumnName(rawProperty);
			selectProperties[i + 1] = newProperty;
		}

		DataFrame propertyTable = grouped.selectExpr(selectProperties);

		// write the final one
		propertyTable.write().mode(SaveMode.Overwrite).format(table_format_parquet)
				.saveAsTable(tablename_complex_property_table);
	}

	/**
	 * Replace all not allowed characters of a DB column name by an
	 * underscore("_") and return a valid DB column name.
	 * 
	 * @param columnName
	 *            column name that will be validated and fixed
	 * @return name of a DB column
	 */
	private String getValidColumnName(String columnName) {
		return columnName.replaceAll("[^a-zA-Z0-9_]", "_");
	}

	/**
	 * Method that contains the full process of creating of complex property
	 * table. Initially, from the rdf triples a table is created. If a file with
	 * prefixes is given, they are replaced. Information of this table is
	 * collected and move to another table named complex_property_table. It
	 * contains one row for each subject and a list of list of its predicates.
	 * Besides a table containing all extracted predicates and their type is
	 * also created and it is named "properties". Finally, the initial triple
	 * table could be deleted if intermediate results are discarded. For more
	 * information about the created and used tables see: TRIPLE TABLE:
	 * {@link ComplexPropertyTableLoader#tablename_triple_table}, PROPERTIES
	 * TABLE: {@link ComplexPropertyTableLoader#tablename_properties}, COMPLEX
	 * PROPERTY TABLE:
	 * {@link ComplexPropertyTableLoader#tablename_complex_property_table}.
	 */
	public void load() {

		buildPrefixMap();
		
		buildTripleTable();

		// create properties table
		savePropertiesIntoTable();

		// collect information for all properties
		Row[] props = this.hiveContext.sql(String.format("SELECT * FROM %s", tablename_properties)).collect();
		String[] allProperties = new String[props.length];
		Boolean[] isComplexProperty = new Boolean[props.length];
		for (int i = 0; i < props.length; i++) {
			allProperties[i] = props[i].getString(0);
			isComplexProperty[i] = props[i].getInt(1) == 1;
		}

		// create complex property table
		buildComplexPropertyTable(allProperties, isComplexProperty);

		// Drop intermediate tables
		if (!keep) {
			dropTables(tablename_triple_table);
		}
	}

	/**
	 * Creates the enormous prefix replace case statements.
	 *
	 * buildTripleStoreTable makes use of regex_replace in its select clause
	 * which is dynamically created for each column.
	 *
	 * Note: Replace this with lambdas in Java 8.
	 *
	 * @param column_name
	 *            The column name for which to create the case statement
	 * 
	 * @return The complete CASE statement for this column
	 */

	private static String prefixHelper(String column_name, Map<String, String> prefix_map) {
		// For each prefix append a case with a regex_replace stmt
		StringBuilder case_clause_builder = new StringBuilder();
		for (Map.Entry<String, String> entry : prefix_map.entrySet()) {
			case_clause_builder.append(String.format(
					"\n\t WHEN %1$s LIKE '%2$s%%'"
							+ "\n\t THEN regexp_replace(translate(%1$s, '<>', ''), '%2$s', '%3$s')",
					column_name, entry.getKey(), entry.getValue()));
		}
		return String.format("CASE %s \n\tELSE %s\n\tEND", case_clause_builder.toString(), column_name);
	}
	
	/**
	 * create the map containing the prefixes, if they are used
	 */
	private void buildPrefixMap() {
		// Read the prefix file if there is one
		this.prefix_map = null;
		if (prefix_file != null) {
			// Get the prefixes and remove braces from long format
			prefix_map = new HashMap<String, String>();
			try {
				BufferedReader br = new BufferedReader(new FileReader(prefix_file));
				for (String line; (line = br.readLine()) != null;) {
					String[] splited = line.split("\\s+");
					if (splited.length < 2) {
						System.out.printf("Line in prefix file has invalid format. Skip. ('%s')\n", line);
						continue;
					}
					prefix_map.put(splited[1].substring(1, splited[1].length() - 1), splited[0]);
				}
				br.close();
			} catch (IOException e) {
				System.err.println("[ERROR] Could not open prefix file. Reason: " + e.getMessage());
				System.exit(1);
			}
		}
	}

	/**
	 * Drop tables.
	 * 
	 * @param tableNames
	 *            list of table names that will be dropped
	 */
	public void dropTables(String... tableNames) {
		for (String tb : tableNames)
			this.hiveContext.sql("DROP TABLE " + tb);
	}

}
