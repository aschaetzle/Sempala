package de.uni_freiburg.informatik.dbis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


/**
 * @author schneidm
 */
public class SempalaNativeLoader {

	public enum TableFormat {
		TRIPLESTORE, PROPERTY_TABLE, EXT_VERT_PART, BIGTABLE
	}

	public static String column_name_subject = "ID";
	public static String column_name_predicate= "predicate";
	public static String column_name_object = "object";
	
	public static void main(String[] args) throws Exception {
	
	    // Create the options
		Options options = buildOptions();

		// Parse the commandline
		CommandLine commandLine;
		try {
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Parsing failed. Reason: " + exp.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "SempalaNativeLoader", options );
			return;
		}
			
        // Dynamically load the impala driver // Why is this not necessary?
//		try {
//	    	Class.forName("com.cloudera.impala.jdbc41.Driver");
//		} catch (ClassNotFoundException e) {
//			System.out.println("Where is your JDBC Driver?");
//			e.printStackTrace();
//			return;
//		}
//	    Enumeration<Driver> d = DriverManager.getDrivers();
//		while (d.hasMoreElements()) {
//			System.out.println(d.nextElement());
//		}

		try {
			// Build the impalad url
			String impalad_url = String.format("jdbc:impala://%s:%s/%s",
					commandLine.getOptionValue("host"),
					commandLine.getOptionValue("port", "21050"),
					commandLine.getOptionValue("database"));
			
			// Establish the connection to impalad
			System.out.println(String.format("Connecting to impalad (%s)", impalad_url));
			Connection connection = DriverManager.getConnection(impalad_url);
			Statement stmt = connection.createStatement();
		    
			// Set impala compression type to snappy
			System.out.println("Setting impala compression type");
			stmt.executeUpdate("set COMPRESSION_CODEC=snappy;");
			
			// Begin performing the requested action
		    String hdfs_input = commandLine.getOptionValue("input");
		    String nt_separator = commandLine.getOptionValue("separator", "\\t");
		    String impala_output = commandLine.getOptionValue("output");
		    String prefixfile = commandLine.getOptionValue("prefixfile", null);
		    switch (commandLine.getOptionValue("format")) {
			case "raw":
				createPrefixedTriplestoreFromHDFSDataset(stmt, hdfs_input, nt_separator, impala_output, prefixfile);
				break;
			case "prop":
				if (prefixfile==null)
					System.out.println("[WARNING]: Building a property table needs prefixed data.");
				final String temporary_table= "sempalaloader_temporary_table";
				createPrefixedTriplestoreFromHDFSDataset(stmt, hdfs_input, nt_separator, temporary_table, prefixfile);
				createPropertyTable(stmt, temporary_table, impala_output);
				// Drop the tmp table
				System.out.println(String.format("Dropping %s", temporary_table));
				stmt.executeUpdate(String.format("DROP TABLE %s;", temporary_table));
				
				break;
			case "extvp":
				// TODO
				break;
			case "big":
				// TODO
				break;
			default:
				System.out.println("Format hst to be one of : raw, prop, extvp, big" );
				System.exit(1);
			}
			connection.close();
		}
		catch (SQLException e)
		{
			System.out.println(e.getLocalizedMessage());
			e.printStackTrace();
			System.exit(1);
		} 
	}
	

	/**
	 * Loads RDF data into an impala parquet table.
	 * 
	 * The input data has to be in N-Triple format and reside in a readable
	 * HDFS directory. The output will be parquet encoded in a raw 
	 * triplestore table with the given name. If a prefixfile is given, the 
	 * matching prefixes in the RDF dataset will be replaced.
	 * 
	 * This function uses solely Impala to accomplish its task.
	 * 
	 * @param stmt The statement to talk to impalad
	 * @param hdfs_input The directory containing the RDF data
	 * @param nt_separator The separator of the triples
	 * @param impala_output The name of the impala table to create
	 * @param prefixfile The file containing the prefixes
	 * @throws SQLException 
	 */
	public static void createPrefixedTriplestoreFromHDFSDataset (Statement stmt, String hdfs_input, String nt_separator, String impala_output, String prefixfile) throws SQLException {
		final String tablename_external_raw = "sempalaloader_external_table";
		
		// Import the table from hdfs into impala
	    System.out.println(String.format("Creating external table '%s' from hdfs location '%s'", tablename_external_raw, hdfs_input));
		stmt.executeUpdate(String.format("CREATE EXTERNAL TABLE %s	 ( %s STRING, %s STRING, %s STRING )\n"
				+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s'\nLOCATION '%s';",
				tablename_external_raw,
				column_name_subject,
				column_name_predicate,
				column_name_object,
				nt_separator,
				hdfs_input));

		// Create a new parquet table, partitioned by predicate");
	    System.out.println(String.format("Creating parquet triplestore table '%s' from '%s'", impala_output, tablename_external_raw));
		stmt.executeUpdate(String.format("CREATE TABLE %s ( %s STRING, %s STRING )\nPARTITIONED BY ( %s STRING ) STORED AS PARQUET;",
				impala_output,
				column_name_subject,
				column_name_object,
				column_name_predicate));
		
		
		// Copy the contents from the external into the internal parquet formatted table
		String insert_stmt;
		if (prefixfile != null){
			
			// Get the prefixes and remove braces from long format 
			Map<String, String> prefixes = new HashMap<String, String>();
			try {
				BufferedReader br = new BufferedReader(new FileReader(prefixfile));
			    for(String line; (line = br.readLine()) != null; ) {
			    	String[] splited = line.split("\\s+");
			    	prefixes.put(splited[2].substring(1, splited[2].length()-1), splited[1]);
			    }
			    br.close();
			} catch (IOException e) {
		        e.printStackTrace();
		        System.exit(1);
			}
				
			// Build the insert statement
			insert_stmt = String.format(
					"INSERT OVERWRITE %s PARTITION (%s)\nSELECT %s, %s, %s\nFROM %s;",
					impala_output,
					column_name_predicate,
					prefixHelper(column_name_subject, prefixes),
					prefixHelper(column_name_object, prefixes),
					prefixHelper(column_name_predicate, prefixes),
					tablename_external_raw
					);
		} else {
			insert_stmt = String.format(
					"INSERT OVERWRITE %1$s PARTITION (%3$s)\nSELECT %2$s, %4$s, %3$s\nFROM %5$s;",
					impala_output,
					column_name_subject,
					column_name_predicate,
					column_name_object,
					tablename_external_raw);
		}
		
		stmt.executeUpdate(insert_stmt);
		
		// Drop the external table
		System.out.println(String.format("Dropping '%s'", tablename_external_raw));
		stmt.executeUpdate(String.format("DROP TABLE %s;", tablename_external_raw));

		// Precompute optimization stats
		System.out.println(String.format("Precomputing optimization stats for '%s'", impala_output));
		stmt.executeUpdate(String.format("COMPUTE STATS %s;", impala_output));
	}
	
	
	/**
	 * Creates the enourmous prefix replace case statements for
	 * createPrefixedTriplestoreFromHDFSDataset.
	 * createPrefixedTriplestoreFromHDFSDataset makes use of regex_replace in
	 * its select clause which is dynamically created for each column. This
	 * function takes over this part to not violate DRY priciple. 
	 * @param column_name The column name for which to create the case statement
	 * @param prefixes The map of prefixes to replace
	 * @return The complete CASE statement for this column
	 */
	public static String prefixHelper(String column_name, Map<String, String> prefixes) {
		// For each prefix append a case with a regex_replace stmt
		StringBuilder case_clause_builder = new StringBuilder();
		for (Map.Entry<String, String> entry : prefixes.entrySet()) {
			case_clause_builder.append(String.format(
					" WHEN %1$s LIKE '<%2$s%%' \n THEN translate(regexp_replace(substr(%1$s, 2, length(%1$s)-2), '%2$s', '%3$s'), ':', '_') \n",
					column_name, entry.getKey(), entry.getValue()));
		}
		return String.format("CASE \n%s ELSE %s\nEND", case_clause_builder.toString(), column_name);
	}

	
	/**
	 * Creates a new property table from a triplestore table. 
	 * 
	 * The input table has to be in triple table format. The output will 
	 * be a table in format described in 'Sempala: Interactive SPARQL Query
	 * Processing on Hadoop'.
	 * 
	 * This function uses solely Impala to accomplish its task.
	 * 
	 * @param stmt The statement to talk to impalad
	 * @param impala_input The name of the impala triplestore table
	 * @param impala_output The name of the impala table to create
	 * @throws SQLException 
	 */
	public static void createPropertyTable(Statement stmt, String impala_input, String impala_output) throws SQLException  {
		final String tablename_distinct_subjects = "distinct_subjects";

		// Create distinct subjects
		System.out.println(String.format("Creating table '%s'", tablename_distinct_subjects));
		stmt.executeUpdate(String.format(
				"CREATE TABLE %s STORED AS PARQUET\nAS SELECT DISTINCT %s\nFROM %s;",
				tablename_distinct_subjects,
				column_name_subject,
				impala_input));
		
		// Precompute optimization stats
		System.out.println(String.format("Precomputing optimization stats for '%s'", tablename_distinct_subjects));
		stmt.executeUpdate(String.format("COMPUTE STATS %s;", tablename_distinct_subjects));
		
		// Prepare for building the property table
		StringBuilder select_clause_builder = new StringBuilder();
		StringBuilder from_clause_builder = new StringBuilder();
		select_clause_builder.append("t1.ID");
		from_clause_builder.append(tablename_distinct_subjects).append(" t1");
	    int table_counter = 2;
		
		// Get all properties
		ResultSet resultSet = stmt.executeQuery(String.format("SELECT DISTINCT %s\nFROM %s;", column_name_predicate, impala_input));
		while (resultSet.next()) {
			String predicate = resultSet.getString("predicate");
			
			// Append ", t<x>.object AS <predicate>" to select clause
			select_clause_builder.append(String.format(
					",\nt%d.%s AS '%s'",
					table_counter,
					column_name_object,
					predicate));

			// Append "LEFT JOIN <tablename_internal_parquet> t<x> ON (t1.ID = t<x>.ID AND t<x>.predicate = <predicate>)" to from clause
			from_clause_builder.append(String.format(
					" LEFT JOIN \n%1$s t%2$d ON (t1.%3$s = t%2$d.%3$s AND t%2$d.%4$s = '%5$s')",
					impala_input,
					table_counter,
					column_name_subject,
					column_name_predicate,
					predicate));
			++table_counter;
		}

		// Create the property table 
		System.out.println(String.format("Creating property table '%s'", impala_output));
		stmt.executeUpdate(String.format("CREATE TABLE %1$s STORED AS PARQUET\nAS SELECT\n%2$s\nFROM\n%3$s",
				impala_output,
				select_clause_builder.toString(),
				from_clause_builder.toString())
				);
		
		// Drop tmp table
		System.out.println(String.format("Dropping '%s'", tablename_distinct_subjects));
		stmt.executeUpdate(String.format("DROP TABLE %s;", tablename_distinct_subjects));

		// Precompute optimization stats
		System.out.println(String.format("Precomputing optimization stats for '%s'", impala_output));
		stmt.executeUpdate(String.format("COMPUTE STATS %s;", impala_output));
	}
	

	public static void createExtVerticalPartitionedTable() {
		// TODO
	}
	

	public static void createBigTable() {
		// TODO
		
	}
	

	public static Options buildOptions() {
		Options options = new Options();
		
		options.addOption(
				Option.builder("i")
				.longOpt("input")
				.desc("The HDFS location of the RDF data (N-Triples).")
				.hasArg()
				.required()
				.build());
		
		options.addOption(
				Option.builder("P")
				.longOpt("prefixfile")
				.desc("The prefixfile in TURTLE format.\nUsed to replace namespaces by prefixes.")
				.hasArg()
				.required(false)
				.build());
		
		options.addOption(
				Option.builder("s")
				.longOpt("separator")
				.desc("The character used to separate the columns in the data. (Defaults to '\\t')")
				.hasArg()
				.required(false)
				.build());
		
		options.addOption(
				Option.builder("h")
				.longOpt("host")
				.desc("The host to connect to.")
				.hasArg()
				.required()
				.build());
		
		options.addOption(
				Option.builder("p")
				.longOpt("port")
				.desc("The port to connect to. (Defaults to 21000)")
				.hasArg()
				.required(false)
				.build());
		
		options.addOption(
				Option.builder("d")
				.longOpt("database")
				.desc("The database to use.")
				.hasArg()
				.required()
				.build());
		
		options.addOption(
				Option.builder("o")
				.longOpt("output")
				.desc("The name of the table to create.")
				.hasArg()
				.required()
				.build());
		
		options.addOption(
				Option.builder("f")
				.longOpt("format")
				.desc("The format to use to create the table.\n"
						+ "raw : The standard triples format partitioned by predicate\n"
						+ "prop : Propertytable (see 'Sempala: Interactive SPARQL Query Processing on Hadoop')\n"
						+ "extvp : Extended Vertical Partitioning (see Master's Thesis: S2RDF, Skilevic Simon\n"
						+ "big : Bigtable (see Master's Thesis: S2RDF, Skilevic Simon")
				.hasArg()
				.required()
				.build());
		
		return options;
	}

	
	
}
