package de.uni_freiburg.informatik.dbis.sempala.loader.impala;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_freiburg.informatik.dbis.sempala.impala.CreateStatement;
import de.uni_freiburg.informatik.dbis.sempala.impala.DataType;
import de.uni_freiburg.informatik.dbis.sempala.impala.FileFormat;
import de.uni_freiburg.informatik.dbis.sempala.impala.Impala;
import de.uni_freiburg.informatik.dbis.sempala.impala.InsertStatement;
import de.uni_freiburg.informatik.dbis.sempala.impala.SelectStatement;


/**
 * 
 * @author schneidm
 *
 */
public class SempalaNativeLoader {

	/** The name used for RDF subject columns */
	public static final String column_name_subject = "ID";

	/** The name used for RDF predicate columns */	
	public static final String column_name_predicate = "predicate";

	/** The name used for RDF object columns */	
	public static final String column_name_object = "object";

	/** The wrapped impala connection */
	private static Impala impala = null;	
		

	/**
	 * Loads RDF data into an impala parquet table.
	 * 
	 * The input data has to be in N-Triple format and reside in a readable
	 * HDFS directory. The output will be parquet encoded in a raw 
	 * triplestore table with the given name. If a prefix file is given, the 
	 * matching prefixes in the RDF dataset will be replaced.
	 * 
	 * @param hdfs_input_directory The directory containing the RDF data
	 * @param tablename_output The name of the impala table to create
	 * @param prefix_map The map containing the prefixes
	 * @param strip_dot Flag if dot at the end of the line is to be stipped
	 * @param field_terminator The separator of the fields in the rdf data 
	 * @param line_terminator The separator of the lines in the rdf data
	 * @throws SQLException
	 */
	public static void buildTripleStoreTable(String hdfs_input_directory, String tablename_output,
			Map<String, String> prefix_map, boolean strip_dot,
			String field_terminator, String line_terminator) throws SQLException {
		
		final String tablename_external_rdf = "rdf_input";
		
		// Import the table from hdfs into impala
	    System.out.println(String.format(
	    		"Creating external table '%s' from hdfs location '%s'",
	    		tablename_external_rdf, hdfs_input_directory));
	    CreateStatement cet = impala.createExternalTable(tablename_external_rdf).ifNotExists()
		.addColumnDefinition(column_name_subject, DataType.STRING)
		.addColumnDefinition(column_name_predicate, DataType.STRING)
		.addColumnDefinition(column_name_object, DataType.STRING)
		.location(hdfs_input_directory);
	    if (field_terminator!=null)
		    cet.fieldTermintor(field_terminator);
	    if (line_terminator!=null)
	    	cet.lineTermintor(line_terminator);
		cet.execute();
		
		// Create a new parquet table, partitioned by predicate");
	    System.out.println(String.format("Creating parquet triplestore table '%s' from '%s'",
	    		tablename_output, tablename_external_rdf));
	    impala.createTable(tablename_output).ifNotExists()
		.storedAs(FileFormat.PARQUET)
		.addColumnDefinition(column_name_subject, DataType.STRING)
		.addColumnDefinition(column_name_object, DataType.STRING)
		.addPartitionDefinition(column_name_predicate, DataType.STRING)
		.execute();
		
	    // First create a select statement for the INSERT statement.
		SelectStatement ss;
		
		// Strip point if necessary (four slashes: one escape for java one for sql
		String column_name_object_dot_stripped = (strip_dot) ?
				String.format("regexp_replace(%s, '\\\\s*\\\\.\\\\s*$', '')", column_name_object) : column_name_object;
		
		// Replace prefixes
		if (prefix_map != null){
			// Build a select statement _WITH_ prefix replaced values
			ss = impala.select(prefixHelper(column_name_subject, prefix_map))
					.addProjection(prefixHelper(column_name_object_dot_stripped, prefix_map))
					.addProjection(prefixHelper(column_name_predicate, prefix_map));
		} else {
			// Build a select statement _WITH_OUT_ prefix replaced values
			ss = impala.select(column_name_subject)
					.addProjection(column_name_object_dot_stripped)
					.addProjection(column_name_predicate);
		}
		ss.from(tablename_external_rdf);
		
		// Now insert the data into the new table
		impala.insertOverwrite(tablename_output).addPartition(column_name_predicate).selectStatement(ss).execute();

		// Drop the external table
		impala.dropTable(tablename_external_rdf);
		
		// Precompute optimization stats
		impala.computeStats(tablename_output);
	}
	
	
	
	/**
	 * Creates the enourmous prefix replace case statements for buildTripleStoreTable.
	 * 
	 * buildTripleStoreTable makes use of regex_replace in its select clause which is dynamically
	 * created for each column. This function takes over this part to not violate DRY principle.
	 * 
	 * Note: Replace this with lambdas in Java 8.
	 *  
	 * @param column_name The column name for which to create the case statement
	 * @param prefix_map The map of prefixes to replace
	 * @return The complete CASE statement for this column
	 */
	public static String prefixHelper(String column_name, Map<String, String> prefix_map) {
		// For each prefix append a case with a regex_replace stmt
		StringBuilder case_clause_builder = new StringBuilder();
		for (Map.Entry<String, String> entry : prefix_map.entrySet()) {
			case_clause_builder.append(String.format(
					"\n WHEN %1$s LIKE '<%2$s%%'"
					+ "\n THEN regexp_replace(translate(%1$s, '<>', ''), '%2$s', '%3$s')",
					column_name, entry.getKey(), entry.getValue()));
		}
		return String.format("CASE %s \nELSE %s\nEND", case_clause_builder.toString(), column_name);
	}

	
	
	/**
	 * Creates a new property table from a triplestore table. 
	 * 
	 * The input table has to be in triple table format. The output will 
	 * be a table in format described in 'Sempala: Interactive SPARQL Query
	 * Processing on Hadoop'.
	 * 
	 * @param hdfs_input_directory The directory containing the RDF data
	 * @param tablename_output The name of the impala table to create
	 * @param prefix_map The map containing the prefixes
	 * @param strip_dot Flag if dot at the end of the line is to be stipped
	 * @param field_terminator The separator of the fields in the rdf data 
	 * @param line_terminator The separator of the lines in the rdf data
	 * @throws SQLException
	 */
	public static void buildPropertyTable(String hdfs_input_directory, String tablename_output, 
			Map<String, String> prefix_map, boolean strip_dot,
			String field_terminator, String line_terminator) throws SQLException {
		
		final String tablename_triplestore = "tmp_triplestore";
		final String tablename_distinct_subjects = "tmp_distinct_subjects";
		
		// Create a table in triple store format from hdfs data
		buildTripleStoreTable(hdfs_input_directory, tablename_triplestore,
				prefix_map, strip_dot, field_terminator,line_terminator);
		
		// Create distinct subjects
		System.out.println(String.format("Creating table '%s'", tablename_distinct_subjects));
		impala.createTable(tablename_distinct_subjects)
		.storedAs(FileFormat.PARQUET)
		.asSelect(
				impala.select(column_name_subject).distinct()
				.from(tablename_triplestore))
		.execute();
		
		// Precompute optimization stats
		impala.computeStats(tablename_distinct_subjects);
		
		// Prepare for building the property table
		SelectStatement ss = impala.select("t1.ID").from(String.format("%s t1", tablename_distinct_subjects));
		int table_counter = 2;
		
		// Get all properties
		ResultSet resultSet = impala.select(column_name_predicate).distinct().from(tablename_triplestore).execute();

		// Iterate over all predicates and build the select and from clauses
		while (resultSet.next()) {
			String predicate = resultSet.getString(column_name_predicate);
			
			// Make the column name impala conform, i.e. remove braces, replace non word chars, trim spaces
			String column_name = predicate.replaceAll("[<>]", "").replaceAll("[[^\\w]+]", "_").trim();
			
			// Append ", t<x>.object AS <predicate>" to select clause
			ss.addProjection(String.format("t%d.%s AS %s", table_counter, column_name_object, column_name));

			// Append "LEFT JOIN <tablename_internal_parquet> t<x> ON (t1.ID = t<x>.ID AND t<x>.predicate = <predicate>)" to from clause
			ss.leftJoin(
					String.format("%s t%d", tablename_triplestore, table_counter),
					String.format("t1.%2$s = t%1$d.%2$s AND t%1$d.%3$s = '%4$s'",
							table_counter, column_name_subject, column_name_predicate, predicate));
			++table_counter;
		}

		// Create the property table 
		impala.createTable(tablename_output).storedAs(FileFormat.PARQUET).asSelect(ss).execute();
		
		// Drop tmp tables
		impala.dropTable(tablename_triplestore);
		impala.dropTable(tablename_distinct_subjects);

		// Precompute optimization stats
		impala.computeStats(tablename_output);
	}

	

	/**
	 * @param hdfs_input_directory The directory containing the RDF data
	 * @param tablename_output The name of the impala table to create
	 * @param prefix_map The map containing the prefixes
	 * @param strip_dot Flag if dot at the end of the line is to be stipped
	 * @param field_terminator The separator of the fields in the rdf data 
	 * @param line_terminator The separator of the lines in the rdf data
	 * @throws SQLException
	 */
	public static void buildExtendedVerticalPartitioningTable(String hdfs_input_directory,
			String tablename_output, Map<String, String> prefix_map, boolean strip_dot,
			String field_terminator, String line_terminator) throws SQLException {
		// TODO		
	}


	/**
	 * @param hdfs_input_directory The directory containing the RDF data
	 * @param tablename_output The name of the impala table to create
	 * @param prefix_map The map containing the prefixes
	 * @param strip_dot Flag if dot at the end of the line is to be stipped
	 * @param field_terminator The separator of the fields in the rdf data 
	 * @param line_terminator The separator of the lines in the rdf data
	 * @throws SQLException
	 */
	public static void buildBigTable(String hdfs_input_directory, String tablename_output,
			Map<String, String> prefix_map, boolean strip_dot,
			String field_terminator, String line_terminator) throws SQLException  {
		// TODO		
	}



	/**
	 * The main routine.
	 * @param args The arguments passed to the program
	 */
	public static void main(String[] args) {
		
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
				.longOpt("prefix-file")
				.desc("The prefix file in TURTLE format.\nUsed to replace namespaces by prefixes.")
				.hasArg()
				.required(false)
				.build());
		
		options.addOption(
				Option.builder("ft")
				.longOpt("field-terminator")
				.desc("The character used to separate the fields in the data. (Defaults to '\\t')")
				.hasArg()
				.required(false)
				.build());
		
		options.addOption(
				Option.builder("lt")
				.longOpt("line-terminator")
				.desc("The character used to separate the lines in the data. (Defaults to '\\n')")
				.hasArg()
				.required(false)
				.build());
		
		options.addOption(
				Option.builder("s")
				.longOpt("strip-dot")
				.desc("Strip th dot in the last field (N-Triples)")
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
				.desc("The port to connect to. (Defaults to 21050)")
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
		
		// Parse the commandline
		CommandLine commandLine = null;
		try {
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			System.err.println("[ERROR] Parsing failed. Reason: " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "SempalaNativeLoader", options );
	        System.exit(1);
		}
		
		// Read the prefix file if there is one
		Map<String, String> prefix_map = null;
		if (commandLine.hasOption("prefix-file")){
			// Get the prefixes and remove braces from long format 
			prefix_map = new HashMap<String, String>();
			try {
				BufferedReader br = new BufferedReader(new FileReader(commandLine.getOptionValue("prefix-file")));
			    for(String line; (line = br.readLine()) != null; ) {
			    	String[] splited = line.split("\\s+");
			    	prefix_map.put(splited[2].substring(1, splited[2].length()-1), splited[1]);
			    }
			    br.close();
			} catch (IOException e) {
				System.err.println("[ERROR] Could not open prefix file. Reason: " + e.getMessage());
		        System.exit(1);
			}
		}

		String host = commandLine.getOptionValue("host");
		String port = commandLine.getOptionValue("port", "21050");
		String database = commandLine.getOptionValue("database");
		String field_terminator = commandLine.getOptionValue("field-terminator", "\\t");
		String line_terminator = commandLine.getOptionValue("line-terminator", "\\n");
		String hdfs_input_directory = commandLine.getOptionValue("input");
		String tablename_output = commandLine.getOptionValue("output");
		String format = commandLine.getOptionValue("format");
		boolean strip_dot = commandLine.hasOption("strip-dot");
		
		try {
			// Connect to impalad
			impala = new Impala(host, port, database);
			
		    switch (format.toLowerCase()) {
			case "raw":
				buildTripleStoreTable(hdfs_input_directory, tablename_output,
						prefix_map, strip_dot, field_terminator, line_terminator);
				break;				
			case "prop":
				buildPropertyTable(hdfs_input_directory, tablename_output,
						prefix_map, strip_dot, field_terminator, line_terminator);
				break;
			case "extvp":
				buildExtendedVerticalPartitioningTable(hdfs_input_directory,
						tablename_output, prefix_map, strip_dot, field_terminator, line_terminator);
				break;
			case "big":
				buildBigTable(hdfs_input_directory, tablename_output,
						prefix_map, strip_dot, field_terminator, line_terminator);
				break;
			default:
				System.err.println("[ERROR] Format has to be one of : raw, prop, extvp, big");
				System.exit(1);
		    }
		}
		catch (SQLException e)
		{
			System.err.println("[ERROR] SQL exception: " + e.getLocalizedMessage());
			System.exit(1);
		} 
	}
}
