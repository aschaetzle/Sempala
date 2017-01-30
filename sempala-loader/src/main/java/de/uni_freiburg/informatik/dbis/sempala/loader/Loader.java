package de.uni_freiburg.informatik.dbis.sempala.loader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import de.uni_freiburg.informatik.dbis.sempala.loader.sql.Impala;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.SelectStatement;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.CreateStatement.DataType;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.CreateStatement.FileFormat;

/**
 * 
 * @author Manuel Schneider <schneidm@informatik.uni-freiburg.de>
 *
 */
public abstract class Loader {
	
	/** The impala wrapper */
	protected Impala impala;

	/*
	 * Input configurations  
	 */
	
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

	/*
	 * Output configurations  
	 */
		
	/** The name of the output table */
	public String tablename_output;

	/** Indicates if shuffle strategy should be used for join operations */
	public boolean shuffle;
	
	/** Indicates if temporary tables must not dropped */
	public boolean keep;

	/** The location of the list of predicates */
	public String path_of_list_of_predicates = "\\n";
	
	/** The types of ExtVP tables*/
	public String extvp_types_selected = "\\n";

	/** The value of threshold*/
	public String threshold;
	
	/** Indicates if Sempala should run in Evaluation Mode */
	public boolean EvaluationMode;
	
	/** Absolut path of users folder in Hdfs */
	public String HdfsUserPath;
	
	/** The constructor */
	public Loader(Impala wrapper, String hdfsLocation) {
		impala = wrapper;
		hdfs_input_directory = hdfsLocation;
	}
		
	/**
	 * Loads RDF data into an impala parquet table.
	 *
	 * The input data has to be in N-Triple format and reside in a readable HDFS
	 * directory. The output will be parquet encoded in a raw triple table
	 * with the given name. If a prefix file is given, the matching prefixes in
	 * the RDF data set will be replaced.
	 * 
	 * @throws SQLException
	 */
	protected void buildTripleTable() throws SQLException {

		final String tablename_external_tripletable = "external_tripletable";

		// Import the table from hdfs into impala
		System.out.println(String.format("Creating external table '%s' from hdfs data", tablename_external_tripletable));
		impala
		.createTable(tablename_external_tripletable)
		.external()
		.addColumnDefinition(column_name_subject, DataType.STRING)
		.addColumnDefinition(column_name_predicate, DataType.STRING)
		.addColumnDefinition(column_name_object, DataType.STRING)
		.fieldTermintor(field_terminator)
		.lineTermintor(line_terminator)
		.location(hdfs_input_directory)
		.execute();
		impala.computeStats(tablename_external_tripletable);

		
		// Read the prefix file if there is one
		Map<String, String> prefix_map = null;
		if (prefix_file != null) {
			// Get the prefixes and remove braces from long format
			prefix_map = new HashMap<String, String>();
			try {
				BufferedReader br = new BufferedReader(new FileReader(prefix_file));
				for (String line; (line = br.readLine()) != null;) {
					String[] splited = line.split("\\s+");
					if (splited.length < 2){
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

		// Create a new parquet table, partitioned by predicate");
		System.out.print(String.format("Creating internal partitioned table '%s' from '%s'", tablename_triple_table, tablename_external_tripletable));
		
		long timestamp = System.currentTimeMillis();
		impala
		.createTable(tablename_triple_table)
		.ifNotExists()
		.storedAs(FileFormat.PARQUET)
		.addColumnDefinition(column_name_subject, DataType.STRING)
		.addColumnDefinition(column_name_object, DataType.STRING)
		.addPartitionDefinition(column_name_predicate, DataType.STRING)
		.execute();

		// First create a select statement for the INSERT statement.
		SelectStatement ss;

		// Strip point if necessary (four slashes: one escape for java one for sql
		String column_name_object_dot_stripped = (strip_dot)
				? String.format("regexp_replace(%s, '\\\\s*\\\\.\\\\s*$', '')", column_name_object)
				: column_name_object;

		// Replace prefixes
		if (prefix_map != null) {
			// Build a select statement _WITH_ prefix replaced values
			ss = impala
					.select(prefixHelper(column_name_subject, prefix_map))
					.addProjection(prefixHelper(column_name_object_dot_stripped, prefix_map))
					.addProjection(prefixHelper(column_name_predicate, prefix_map));
		} else {
			// Build a select statement _WITH_OUT_ prefix replaced values
			ss = impala
					.select(column_name_subject)
					.addProjection(column_name_object_dot_stripped)
					.addProjection(column_name_predicate);
		}
		if (unique)
			ss.distinct();
		ss.from(tablename_external_tripletable);

		// Now insert the data into the new table
		impala
		.insertOverwrite(tablename_triple_table)
		.addPartition(column_name_predicate)
		.selectStatement(ss)
		.execute();
		System.out.println(String.format(" [%.3fs]", (float)(System.currentTimeMillis() - timestamp)/1000));
		impala.computeStats(tablename_triple_table);

		// Drop intermediate tables
		if (!keep)
			impala.dropTable(tablename_external_tripletable);
	}

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
	 * @param column_name The column name for which to create the case statement
	 * 
	 * @return The complete CASE statement for this column
	 */
	private static String prefixHelper(String column_name, Map<String, String> prefix_map) {
		// For each prefix append a case with a regex_replace stmt
		StringBuilder case_clause_builder = new StringBuilder();
		for (Map.Entry<String, String> entry : prefix_map.entrySet()) {
			case_clause_builder.append(String.format(
					"\n\t WHEN %1$s LIKE '<%2$s%%'" + "\n\t THEN regexp_replace(translate(%1$s, '<>', ''), '%2$s', '%3$s')",
					column_name, entry.getKey(), entry.getValue()));
		}
		return String.format("CASE %s \n\tELSE %s\n\tEND", case_clause_builder.toString(), column_name);
	}
	
	/**
	 * Makes the string conform to the requirements for impala column names.
	 * I.e. remove braces, replace non word characters, trim spaces.
	 * @param The string to make impala conform
	 * @return The impala conform string
	 */
	protected static String toImpalaColumnName(String s) {
		// Space is a nonword character so trim before replacing chars.
		return s.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
	}

	/** Abstract method that has to be implemented */
	public abstract void load() throws SQLException;
}
