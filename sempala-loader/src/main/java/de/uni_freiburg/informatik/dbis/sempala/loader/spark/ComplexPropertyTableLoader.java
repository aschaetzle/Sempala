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

import de.uni_freiburg.informatik.dbis.sempala.loader.udf.PropertiesAggregateFunction;

// TODO add class comments - TODO THIS IS NOT A FINAL VERSION OF THE CLASS 
public class ComplexPropertyTableLoader {

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
	// TODO add comments
	protected static final String tablename_properties = "properties";
	protected static final String tablename_complex_property_table = "complex_property_table";
	protected static final String column_name_is_complex = "is_complex";

	/** The name used for RDF subject columns. */
	public String column_name_subject = "s";

	/** The name used for RDF predicate columns. */
	public String column_name_predicate = "p";

	/** The name used for RDF object columns. */
	public String column_name_object = "o";

	protected static final String table_format_parquet = "parquet";

	/** Indicates if temporary tables must not dropped. */
	public boolean keep;

	private Spark connection;
	private HiveContext hiveContext;

	public ComplexPropertyTableLoader(Spark connection, String hdfsLocation) {
		this.connection = connection;
		this.hiveContext = connection.getHiveContext();
		this.hdfs_input_directory = hdfsLocation;
	}

	// TODO add comments
	private static String getValidName(String name) {
		return name.replaceAll("[^a-zA-Z0-9_]", "_");
	}

	// TODO add comments
	public void buildTripleTable() {

		// TODO remove this after testing
		// hc.sql("CREATE EXTERNAL TABLE triple_table(s STRING, p STRING, o
		// STRING) "
		// + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " + "LOCATION '" +
		// path + "'");

		// create initial table from a rdf file
		final String tablename_external_tripletable = "external_tripletable";
		String createExternalTable = String.format(
				"CREATE EXTERNAL TABLE %s(%s STRING, %s STRING, %s STRING) ROW FORMAT DELIMITED"
						+ " FIELDS TERMINATED BY '%s'  LINES TERMINATED BY '%s' LOCATION '%s'",
				tablename_external_tripletable, column_name_subject, column_name_predicate, column_name_object,
				field_terminator, line_terminator, hdfs_input_directory);

		this.hiveContext.sql(createExternalTable);

		// Read the prefix file if there is one
		Map<String, String> prefix_map = null;
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
		
		// Strip point if necessary (four slashes: one escape for java one for
		// sql
		String column_name_object_dot_stripped = (strip_dot)
				? String.format("regexp_replace(%s, '\\\\s*\\\\.\\\\s*$', '')", column_name_object)
				: column_name_object;

		// select from the initial triple table replacing the prefixes
		StringBuilder selectStatement = new StringBuilder();
		selectStatement.append("SELECT ");
		if (unique) {
			selectStatement.append("DISTINCT ");
		}
		
		String projectionSubject = null;
		String projectionObject = null;
		String projectionPredicate = null;
		// Replace prefixes
		if (prefix_map != null) {
			// Build a select statement _WITH_ prefix replaced values
			// TODO TEST IF this works
	
			projectionSubject = prefixHelper(column_name_subject, prefix_map);
			projectionObject = prefixHelper(column_name_object_dot_stripped, prefix_map);
			projectionPredicate = prefixHelper(column_name_predicate, prefix_map);
			 
		} else {
			// Build a select statement _WITH_OUT_ prefix replaced values
			projectionSubject = column_name_subject;
			projectionObject = column_name_object_dot_stripped;
			projectionPredicate = column_name_predicate;
		}
		selectStatement.append(projectionSubject + " AS " + column_name_subject +", ");
		selectStatement.append(projectionPredicate  + " AS " + column_name_predicate + ", ");
		selectStatement.append(projectionObject + " AS " + column_name_object);
		selectStatement.append(" FROM " + tablename_external_tripletable);
		DataFrame triples = this.hiveContext.sql(selectStatement.toString());
		
		// save triples with prefixes replaced
		triples.write().mode(SaveMode.Overwrite).saveAsTable(tablename_triple_table);
			
		// Drop intermediate tables
		if (!keep) {
			dropTemporaryTables(tablename_external_tripletable);
		}
	}

	// TODO add comments
	public void savePropertiesIntoTable(String tableName) {
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

		// write the result
		combinedProperties.write().mode(SaveMode.Overwrite).saveAsTable(tableName);
	}

	/*
	 * Create the final property table, allProperties contains the list of all
	 * possible properties isComplexProperty contains (in the same order used by
	 * allProperties) the boolean value that indicates if that property is
	 * complex (called also multi valued) or simple.
	 */
	public void buildPropertyTable(String[] allProperties, Boolean[] isComplexProperty) {

		// create a new aggregation environment
		PropertiesAggregateFunction aggregator = new PropertiesAggregateFunction(allProperties);

		String predicateObjectColumn = "po";
		String groupColumn = "group";

		// get the compressed table
		DataFrame compressedTriples = this.hiveContext.sql(String.format("SELECT %s, CONCAT(%s, ' ', %s) AS po FROM %s",
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

		buildTripleTable();

		// table name - properties
		savePropertiesIntoTable(tablename_properties);

		Row[] props = this.hiveContext.sql(String.format("SELECT * FROM %s", tablename_properties)).collect();
		String[] allProperties = new String[props.length];
		Boolean[] isComplexProperty = new Boolean[props.length];
		for (int i = 0; i < props.length; i++) {
			allProperties[i] = props[i].getString(0);
			isComplexProperty[i] = props[i].getInt(1) == 1;
		}

		buildPropertyTable(allProperties, isComplexProperty);

		// Drop intermediate tables
		if (!keep) {
			dropTemporaryTables(tablename_triple_table);
		}
	}

	// TODO change comments
	/**
	 * Creates the enormous prefix replace case statements for
	 * buildTripleStoreTable.
	 *
	 * buildTripleStoreTable makes use of regex_replace in its select clause
	 * which is dynamically created for each column. This function takes over
	 * this part to not violate DRY principle.
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
					"\n\t WHEN %1$s LIKE '<%2$s%%'"
							+ "\n\t THEN regexp_replace(translate(%1$s, '<>', ''), '%2$s', '%3$s')",
					column_name, entry.getKey(), entry.getValue()));
		}
		return String.format("CASE %s \n\tELSE %s\n\tEND", case_clause_builder.toString(), column_name);
	}

	// drop all the unnecessary tables
	public void dropTemporaryTables(String... tableNames) {
		for (String tb : tableNames)
			this.hiveContext.sql("DROP TABLE " + tb);
	}

}
