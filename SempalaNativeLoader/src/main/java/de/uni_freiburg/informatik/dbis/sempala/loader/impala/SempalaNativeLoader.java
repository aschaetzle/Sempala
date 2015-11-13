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
import org.apache.commons.lang.NotImplementedException;

import de.uni_freiburg.informatik.dbis.sempala.impala.DataType;
import de.uni_freiburg.informatik.dbis.sempala.impala.FileFormat;
import de.uni_freiburg.informatik.dbis.sempala.impala.Impala;
import de.uni_freiburg.informatik.dbis.sempala.impala.QueryOption;
import de.uni_freiburg.informatik.dbis.sempala.impala.SelectStatement;


/**
 * 
 * @author schneidm
 *
 */
public class SempalaNativeLoader {

	private static final String column_name_subject = "ID";	
	private static final String column_name_predicate= "predicate";	
	private static final String column_name_object = "object";

	private static String hdfs_input = null;
	private static String nt_separator = null;
	private static String impala_output = null;
	private static String prefixfile = null;
	private static String host = null;
	private static String port = null;
	private static String database = null;
	private static Map<String, String> prefixes = null;
	
	
	
	/**
	 * The main routine.
	 * @param args The arguments passed to the program
	 */
	public static void main(String[] args) {
	
	    // Create the options
		Options options = buildOptions();

		// Parse the commandline
		CommandLine commandLine;
		try {
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("Parsing failed. Reason: " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "SempalaNativeLoader", options );
			return;
		}

		try {
			// Get opts
			hdfs_input    = commandLine.getOptionValue("input");
			nt_separator  = commandLine.getOptionValue("separator", "\\t");
			impala_output = commandLine.getOptionValue("output");
			prefixfile    = commandLine.getOptionValue("prefixfile", null);
			host          = commandLine.getOptionValue("host");
			port          = commandLine.getOptionValue("port", "21050");
			database      = commandLine.getOptionValue("database");
		    
		    // Read the prefix file if there is one
			if (prefixfile != null){
				Map<String, String> prefixes = new HashMap<String, String>();
				// Get the prefixes and remove braces from long format 
				try {
					BufferedReader br = new BufferedReader(new FileReader(prefixfile));
				    for(String line; (line = br.readLine()) != null; ) {
				    	String[] splited = line.split("\\s+");
				    	prefixes.put(splited[2].substring(1, splited[2].length()-1), splited[1]);
				    }
				    br.close();
				} catch (IOException e) {
			        e.getLocalizedMessage();
			        System.exit(1);
				}
			}

			// Connect to impalad
			Impala impala = new Impala(host, port, database);
		    
			// Set impala compression type to snappy
			impala.set(QueryOption.COMPRESSION_CODEC, "SNAPPY");
		    
		    switch (commandLine.getOptionValue("format")) {
			case "raw":
				// Create a table in triple store format from hdfs data
				createPrefixedTriplestoreFromHDFSDataset(impala, hdfs_input, nt_separator, impala_output, prefixfile);
				break;
				
			case "prop":
				final String temporary_table= "sempalaloader_temporary_table";

				// Warn the user if no prefix file is given. Propertytable need prefixded data.
				if (prefixfile==null)
					System.out.println("[WARNING]: Building a property table needs prefixed data.");
				
				// Create a table in triple store format from hdfs data
				createPrefixedTriplestoreFromHDFSDataset(impala, hdfs_input, nt_separator, temporary_table, prefixfile);
				
				// Create a property table from the triple store
				createPropertyTable(impala, temporary_table, impala_output);
				
				// Drop the tmp table
				impala.dropTable(temporary_table);
				break;
				
			case "extvp":
				// TODO
				throw new NotImplementedException("EXT_VP gibts noch nicht.");
				
			case "big":
				// TODO
				throw new NotImplementedException("BigTable gibts noch nicht.");
				
			default:
				System.out.println("Format hst to be one of : raw, prop, extvp, big" );
				System.exit(1);
			}
			impala.disconnect();
		}
		catch (SQLException e)
		{
			System.out.println(e.getLocalizedMessage());
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
	public static void createPrefixedTriplestoreFromHDFSDataset (Impala impala, String hdfs_input, String nt_separator, String impala_output, String prefixfile) throws SQLException {
		final String tablename_external_raw = "sempalaloader_external_table";
		
		// Import the table from hdfs into impala
	    System.out.println(String.format("Creating external table '%s' from hdfs location '%s'", tablename_external_raw, hdfs_input));
	    impala.createExternalTable(tablename_external_raw)
		.addColumnDefinition(column_name_subject, DataType.STRING)
		.addColumnDefinition(column_name_predicate, DataType.STRING)
		.addColumnDefinition(column_name_object, DataType.STRING)
		.fieldTermintor(nt_separator)
		.location(hdfs_input)
		.execute();
		
		// Create a new parquet table, partitioned by predicate");
	    System.out.println(String.format("Creating parquet triplestore table '%s' from '%s'", impala_output, tablename_external_raw));
	    impala.createTable(impala_output)
		.addColumnDefinition(column_name_subject, DataType.STRING)
		.addColumnDefinition(column_name_object, DataType.STRING)
		.addPartitionDefinition(column_name_predicate, DataType.STRING)
		.storedAs(FileFormat.PARQUET)
		.execute();
		
	    // First create a select statement for the INSERT statement.
		SelectStatement ss;
		if (prefixfile != null){
			// Build a select statement _WITH_ prefix replaced values
			ss = impala.select(prefixHelper(column_name_subject, prefixes))
					.addProjection(prefixHelper(column_name_object, prefixes))
					.addProjection(prefixHelper(column_name_predicate, prefixes));
		} else {
			// Build a select statement _WITH_OUT_ prefix replaced values
			ss = impala.select(column_name_subject)
					.addProjection(column_name_object)
					.addProjection(column_name_predicate);
		}
		ss.from(tablename_external_raw);
		
		// Now insert the data into the new table
		impala.insertOverwrite(impala_output).addPartition(column_name_predicate).selectStatement(ss).execute();

		// Drop the external table
		impala.dropTable(tablename_external_raw);
		
		// Precompute optimization stats
		impala.computeStats(impala_output);
	}
	
	
	
	/**
	 * Creates the enourmous prefix replace case statements for
	 * createPrefixedTriplestoreFromHDFSDataset.
	 * 
	 * createPrefixedTriplestoreFromHDFSDataset makes use of regex_replace in
	 * its select clause which is dynamically created for each column. This
	 * function takes over this part to not violate DRY priciple.
	 *  
	 * @param column_name The column name for which to create the case statement
	 * @param prefixes The map of prefixes to replace
	 * @return The complete CASE statement for this column
	 * @deprecated
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
	public static void createPropertyTable(Impala impala, String impala_input, String impala_output) throws SQLException  {
		final String tablename_distinct_subjects = "distinct_subjects";

		
		// Create distinct subjects
		System.out.println(String.format("Creating table '%s'", tablename_distinct_subjects));
		impala.createTable(tablename_distinct_subjects)
		.storedAs(FileFormat.PARQUET)
		.asSelect(
				impala.select(column_name_subject).distinct()
				.from(impala_input))
		.execute();
		
		// Precompute optimization stats
		impala.computeStats(tablename_distinct_subjects);
		
		// Prepare for building the property table
		SelectStatement ss = impala.select("t1.ID");
		StringBuilder from_clause_builder = new StringBuilder();
		// TODO THIS MUST BE POSSIBLE IN JAVA
		from_clause_builder.append(tablename_distinct_subjects).append(" t1");
	    int table_counter = 2;
		
		// Get all properties
		ResultSet resultSet = impala.select(column_name_predicate).distinct().from(impala_input).execute();
				
		while (resultSet.next()) {
			String predicate = resultSet.getString(column_name_predicate);
			
			// Append ", t<x>.object AS <predicate>" to select clause
			ss.addProjection(String.format(",\nt%d.%s AS '%s'", table_counter, column_name_object, predicate));

			// TODO THIS MUST BE POSSIBLE IN JAVA
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
		ss.from(from_clause_builder.toString());
		impala.createTable(impala_output).storedAs(FileFormat.PARQUET).asSelect(ss).execute();
		
		// Drop tmp table
		impala.dropTable(tablename_distinct_subjects);

		// Precompute optimization stats
		impala.computeStats(impala_output);
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
				Option.builder("t")
				.longOpt("line-terminator")
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
		
		return options;
	}

	
	
}
