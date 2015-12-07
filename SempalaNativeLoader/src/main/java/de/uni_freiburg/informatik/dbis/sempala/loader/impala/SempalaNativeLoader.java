package de.uni_freiburg.informatik.dbis.sempala.loader.impala;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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

	/** The impala wrapper */
	private static Impala impala;

	/** The separator of the fields in the rdf data */
	private static String field_terminator;

	/** The separator of the lines in the rdf data */
	private static String line_terminator;

	/** Flag if dot at the end of the line is to be stripped */
	private static String hdfs_input_directory;

	/** Flag if dot at the end of the line is to be stripped */
	private static String tablename_output;

	/** Flag if dot at the end of the line is to be stripped */
	private static boolean strip_dot;

	/** Flag if duplicates in the input are to be ignored */
	private static boolean unique;

	/** The map containing the prefixes */
	private static Map<String, String> prefix_map = null;

	/**
	 * Loads RDF data into an impala parquet table.
	 * 
	 * The input data has to be in N-Triple format and reside in a readable HDFS
	 * directory. The output will be parquet encoded in a raw triplestore table
	 * with the given name. If a prefix file is given, the matching prefixes in
	 * the RDF dataset will be replaced.
	 * 
	 * @param hdfs_input_directory
	 *            The directory containing the RDF data
	 * @param tablename_output
	 *            The name of the impala table to create
	 * @throws SQLException
	 */
	public static void buildTripleStoreTable(String hdfs_input_directory, String tablename_output) throws SQLException {

		final String tablename_external_rdf = "external_rdf_data";

		// Import the table from hdfs into impala
		CreateStatement cet = impala
				.createTable(tablename_external_rdf)
				.external()
				.ifNotExists()
				.addColumnDefinition(column_name_subject, DataType.STRING)
				.addColumnDefinition(column_name_predicate, DataType.STRING)
				.addColumnDefinition(column_name_object, DataType.STRING)
				.location(hdfs_input_directory);
		if (field_terminator != null)
			cet.fieldTermintor(field_terminator);
		if (line_terminator != null)
			cet.lineTermintor(line_terminator);
		cet.execute();

		// Create a new parquet table, partitioned by predicate");
		impala
		.createTable(tablename_output)
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
					.select(prefixHelper(column_name_subject))
					.addProjection(prefixHelper(column_name_object_dot_stripped))
					.addProjection(prefixHelper(column_name_predicate));
		} else {
			// Build a select statement _WITH_OUT_ prefix replaced values
			ss = impala
					.select(column_name_subject)
					.addProjection(column_name_object_dot_stripped)
					.addProjection(column_name_predicate);
		}
		if (unique)
			ss.distinct();
		ss.from(tablename_external_rdf);

		// Now insert the data into the new table
		impala
		.insertOverwrite(tablename_output)
		.addPartition(column_name_predicate)
		.selectStatement(ss)
		.execute();

		// Drop the external table
		impala.dropTable(tablename_external_rdf);

		// Precompute optimization stats
		impala.computeStats(tablename_output);
	}

	/**
	 * Creates the enourmous prefix replace case statements for
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
	 * @return The complete CASE statement for this column
	 */
	public static String prefixHelper(String column_name) {
		// For each prefix append a case with a regex_replace stmt
		StringBuilder case_clause_builder = new StringBuilder();
		for (Map.Entry<String, String> entry : prefix_map.entrySet()) {
			case_clause_builder.append(String.format(
					"\n WHEN %1$s LIKE '<%2$s%%'" + "\n THEN regexp_replace(translate(%1$s, '<>', ''), '%2$s', '%3$s')",
					column_name, entry.getKey(), entry.getValue()));
		}
		return String.format("CASE %s \nELSE %s\nEND", case_clause_builder.toString(), column_name);
	}

	/**
	 * Creates a new property table from a triplestore table.
	 * 
	 * The input table has to be in triple table format. The output will be a
	 * table in format described in 'Sempala: Interactive SPARQL Query
	 * Processing on Hadoop'.
	 * 
	 * @param hdfs_input_directory
	 *            The directory containing the RDF data
	 * @param tablename_output
	 *            The name of the impala table to create
	 * @throws SQLException
	 */
	public static void buildPropertyTable(String hdfs_input_directory, String tablename_output) throws SQLException {

		final String tablename_triplestore = "tmp_triplestore";
		final String tablename_distinct_subjects = "tmp_distinct_subjects";

		// Create a table in triple store format from hdfs data
		buildTripleStoreTable(hdfs_input_directory, tablename_triplestore);

		// Create distinct subjects
		impala
		.createTable(tablename_distinct_subjects)
		.storedAs(FileFormat.PARQUET)
		.asSelect(
				impala
				.select(column_name_subject)
				.distinct()
				.from(tablename_triplestore)
				)
		.execute();

		// Precompute optimization stats
		impala.computeStats(tablename_distinct_subjects);

		// Prepare for building the property table
		SelectStatement ss = impala
				.select("t1."+ column_name_subject)
				.from(String.format("%s t1", tablename_distinct_subjects));

		// Get all properties
		ResultSet resultSet = impala
				.select(column_name_predicate)
				.distinct()
				.from(tablename_triplestore)
				.execute();

		int table_counter = 2;
		
		// Iterate over all predicates and build the select and from clauses
		while (resultSet.next()) {
			String predicate = resultSet.getString(column_name_predicate);

			// Make the column name impala conform, i.e. remove braces, replace
			// non word chars, trim spaces
			String column_name = toImpalaColName(predicate);

			// Append ", t<x>.object AS <predicate>" to select clause
			ss.addProjection(String.format("t%d.%s AS %s", table_counter, column_name_object, column_name));

			// Append "LEFT JOIN <tablename_internal_parquet> t<x> ON (t1.subject =
			// t<x>.subject AND t<x>.predicate = <predicate>)" to from clause
			ss.leftJoin(String.format("%s t%d", tablename_triplestore, table_counter),
					String.format("t1.%2$s = t%1$d.%2$s AND t%1$d.%3$s = '%4$s'", table_counter, column_name_subject,
							column_name_predicate, predicate));
			++table_counter;
		}

		// Create the property table
		impala
		.createTable(tablename_output)
		.storedAs(FileFormat.PARQUET)
		.asSelect(ss)
		.execute();

		// Drop tmp tables
		impala.dropTable(tablename_triplestore);
		impala.dropTable(tablename_distinct_subjects);

		// Precompute optimization stats
		impala.computeStats(tablename_output);
	}

	/**
	 * @param hdfs_input_directory
	 *            The directory containing the RDF data
	 * @param tablename_output
	 *            The name of the impala table to create
	 * @throws SQLException
	 */
	public static void buildExtendedVerticalPartitioningTable(String hdfs_input_directory, String tablename_output)
			throws SQLException {
		// TODO
	}

	/**
	 * @param hdfs_input_directory The directory containing the RDF data
	 * @param tablename_output The name of the impala table to create
	 * @throws SQLException
	 */
	public static void buildSingleTable(String hdfs_input_directory, String tablename_output) throws SQLException  {
		
		final String tablename_triplestore = "tmp_triplestore";
		final String tablename_subjects = "tmp_subjects";
		final String tablename_objects  = "tmp_objects";
		
		// Create a table in triple store format from hdfs data
		buildTripleStoreTable(hdfs_input_directory, tablename_triplestore);

		// Create a table for the distinct subject-predicate tuples
		impala
		.createTable(tablename_subjects)
		.ifNotExists()
		.storedAs(FileFormat.PARQUET)
		.addColumnDefinition(column_name_subject, DataType.STRING)
		.addPartitionDefinition(column_name_predicate, DataType.STRING)
		.execute();

		// Fill the table with distinct subject-predicate tuples
		impala
		.insertOverwrite(tablename_subjects)
		.addPartition(column_name_predicate)
		.selectStatement(
				impala
				.select(column_name_subject)
				.addProjection(column_name_predicate)
				.distinct()
				.from(tablename_triplestore)
				)
		.execute();
		
		// Precompute optimization stats
		impala.computeStats(tablename_subjects);
		
		// Create a table for the distinct predicate-object tuples
		impala
		.createTable(tablename_objects)
		.ifNotExists()
		.storedAs(FileFormat.PARQUET)
		.addColumnDefinition(column_name_object, DataType.STRING)
		.addPartitionDefinition(column_name_predicate, DataType.STRING)
		.execute();

		// Fill the table with distinct predicate-object tuples
		impala
		.insertOverwrite(tablename_objects)
		.addPartition(column_name_predicate)
		.selectStatement(
				impala
				.select(column_name_object)
				.addProjection(column_name_predicate)
				.distinct()
				.from(tablename_triplestore)
				)
		.execute();
		
		// Precompute optimization stats
		impala.computeStats(tablename_objects);
		
		// Get all properties
		ResultSet resultSet = impala
				.select(column_name_predicate)
				.distinct()
				.from(tablename_triplestore)
				.execute();
		
		// Convert the result set to a list
		ArrayList<String> predicates = new ArrayList<String>();
		while (resultSet.next())
			predicates.add(resultSet.getString(column_name_predicate));
		
		// Create the new single table "s, p, o, [ss_p1, so_p1, os_p1], ..."
		CreateStatement cstmt = impala
				.createTable(tablename_output)
				.ifNotExists()
				.addColumnDefinition(column_name_subject, DataType.STRING)
				.addPartitionDefinition(column_name_predicate, DataType.STRING)
				.addColumnDefinition(column_name_object, DataType.STRING);
		for (String pred : predicates){
			String impalaConformPred = toImpalaColName(pred);
			cstmt.addColumnDefinition(String.format("ss_%s", impalaConformPred), DataType.BOOLEAN);
			cstmt.addColumnDefinition(String.format("so_%s", impalaConformPred), DataType.BOOLEAN);
			cstmt.addColumnDefinition(String.format("os_%s", impalaConformPred), DataType.BOOLEAN);
		}
		cstmt.storedAs(FileFormat.PARQUET);
		cstmt.execute();
		
		/*
		 * Build a select stmt for the Insert-as-select statement 
		 */
		SelectStatement sstmt = impala.select().distinct();
		
		// Build the projection part
		sstmt.addProjection(String.format("nt.%s", column_name_subject));
		sstmt.addProjection(String.format("nt.%s", column_name_object));
		for (String pred : predicates){
			String impalaConformPred = toImpalaColName(pred);
			// SS_pi
			sstmt.addProjection(String.format("CASE WHEN %s.%s IS NULL THEN false ELSE true END AS %s",
					String.format("tss_%s", impalaConformPred),
					column_name_subject,
					String.format("ss_%s", impalaConformPred)));
			// SO_pi
			sstmt.addProjection(String.format("CASE WHEN %s.%s IS NULL THEN false ELSE true END AS %s",
					String.format("tso_%s", impalaConformPred),
					column_name_object,
					String.format("so_%s", impalaConformPred)));
			// OS_pi
			sstmt.addProjection(String.format("CASE WHEN %s.%s IS NULL THEN false ELSE true END AS %s",
					String.format("tos_%s", impalaConformPred),
					column_name_subject,
					String.format("os_%s", impalaConformPred)));
		}
		// Partition column at last (impala requirement)
		sstmt.addProjection(String.format("nt.%s", column_name_predicate));
		
		// Build the table references
		sstmt.from(String.format("%s nt", tablename_triplestore));
		for (String predicate : predicates){
			String impalaConformPredicate = toImpalaColName(predicate);
			
			// SS_pi
			sstmt.leftJoin(
					// Table reference e.g. "LEFT JOIN subjects tss_p1"
					String.format("%s tss_%s", tablename_subjects, impalaConformPredicate),
					// On clause e.g. "ON nt.id=tss_p1.id AND tss_p1.predicate='wsdbm:friendOf'"
					String.format("nt.%1$s=tss_%3$s.%1$s AND tss_%3$s.%2$s='%4$s'",
							column_name_subject,
							column_name_predicate,
							impalaConformPredicate,
							predicate));
			
			// SO_pi
			sstmt.leftJoin(
					// Table reference e.g. "LEFT JOIN objects tso_p1"
					String.format("%s tso_%s", tablename_objects, impalaConformPredicate),
					// On clause e.g. "ON nt.id=tso_p1.object AND tso_p1.predicate='wsdbm:friendOf'"
					String.format("nt.%1$s=tso_%4$s.%3$s AND tso_%4$s.%2$s='%5$s'",
							column_name_subject,
							column_name_predicate,
							column_name_object,
							impalaConformPredicate,
							predicate));
			
			// OS_pi
			sstmt.leftJoin(
					// Table reference e.g. "LEFT JOIN subjects tos_p1"
					String.format("%s tos_%s", tablename_subjects, impalaConformPredicate),
					// On clause e.g. "ON nt.object=tos_p1.id AND tos_p1.predicate='wsdbm:friendOf'"
					String.format("nt.%3$s=tos_%4$s.%1$s AND tos_%4$s.%2$s='%5$s'",
							column_name_subject,
							column_name_predicate,
							column_name_object,
							impalaConformPredicate,
							predicate));
		}
	

		// Insert data into the single table using the built select stmt
		impala
		.insertOverwrite(tablename_output)
		.addPartition(column_name_predicate)
		.selectStatement(sstmt)
		.execute();

		// Drop the temporary tables
		impala.dropTable(tablename_triplestore);
		impala.dropTable(tablename_subjects);
		impala.dropTable(tablename_objects);

		// Precompute optimization stats
		impala.computeStats(tablename_output);
	}
	
	/**
	 * Makes the string conform to the requirements for impala column names.
	 * I.e. remove braces, replace non word characters, trim spaces.
	 * @param The string to make impala conform
	 * @return The impala conform string
	 */
	public static String toImpalaColName(String s) {
		// Space is a nonword character so trim before replacing chars.
		return s.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
	}
	
	/**
	 * The main routine.
	 * 
	 * @param args
	 *            The arguments passed to the program
	 */
	public static void main(String[] args) {

		Options options = new Options();

		options.addOption(Option.builder("i").longOpt("input").desc("The HDFS location of the RDF data (N-Triples).")
				.hasArg().required().build());

		options.addOption(Option.builder("P").longOpt("prefix-file")
				.desc("The prefix file in TURTLE format.\nUsed to replace namespaces by prefixes.").hasArg()
				.required(false).build());

		options.addOption(Option.builder("ft").longOpt("field-terminator")
				.desc("The character used to separate the fields in the data. (Defaults to '\\t')").hasArg()
				.required(false).build());

		options.addOption(Option.builder("lt").longOpt("line-terminator")
				.desc("The character used to separate the lines in the data. (Defaults to '\\n')").hasArg()
				.required(false).build());

		options.addOption(Option.builder("s").longOpt("strip-dot").desc("Strip th dot in the last field (N-Triples)")
				.required(false).build());

		options.addOption(
				Option.builder("u").longOpt("unique").desc("Ignore dups from the input").required(false).build());

		options.addOption(
				Option.builder("h").longOpt("host").desc("The host to connect to.").hasArg().required().build());

		options.addOption(Option.builder("p").longOpt("port").desc("The port to connect to. (Defaults to 21050)")
				.hasArg().required(false).build());

		options.addOption(
				Option.builder("d").longOpt("database").desc("The database to use.").hasArg().required().build());

		options.addOption(Option.builder("o").longOpt("output").desc("The name of the table to create.").hasArg()
				.required().build());

		options.addOption(Option.builder("f").longOpt("format")
				.desc("The format to use to create the table.\n"
						+ "raw : The standard triples format partitioned by predicate\n"
						+ "prop : Propertytable (see 'Sempala: Interactive SPARQL Query Processing on Hadoop')\n"
						+ "extvp : (Not implemented)\n" //Extended Vertical Partitioning (see Master's Thesis: S2RDF, Skilevic Simon\n"
						+ "single : Singletable (see Master's Thesis: S2RDF, Skilevic Simon)")
				.hasArg().required().build());

		// Parse the commandline
		CommandLine commandLine = null;
		try {
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			System.err.println("[ERROR] Parsing failed. Reason: " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("SempalaNativeLoader", options);
			System.exit(1);
		}

		// Read the prefix file if there is one
		if (commandLine.hasOption("prefix-file")) {
			// Get the prefixes and remove braces from long format
			prefix_map = new HashMap<String, String>();
			try {
				BufferedReader br = new BufferedReader(new FileReader(commandLine.getOptionValue("prefix-file")));
				for (String line; (line = br.readLine()) != null;) {
					String[] splited = line.split("\\s+");
					prefix_map.put(splited[1].substring(1, splited[1].length() - 1), splited[0]);
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
		String format = commandLine.getOptionValue("format");
		hdfs_input_directory = commandLine.getOptionValue("input");
		tablename_output = commandLine.getOptionValue("output");
		field_terminator = commandLine.getOptionValue("field-terminator", "\\t");
		line_terminator = commandLine.getOptionValue("line-terminator", "\\n");
		strip_dot = commandLine.hasOption("strip-dot");
		unique = commandLine.hasOption("unique");

		try {
			// Connect to impalad
			impala = new Impala(host, port, database);

			switch (format.toLowerCase()) {
			case "raw":
				buildTripleStoreTable(hdfs_input_directory, tablename_output);
				break;
			case "prop":
				buildPropertyTable(hdfs_input_directory, tablename_output);
				break;
			case "extvp":
				buildExtendedVerticalPartitioningTable(hdfs_input_directory, tablename_output);
				break;
			case "single":
				buildSingleTable(hdfs_input_directory, tablename_output);
				break;
			default:
				System.err.println("[ERROR] Format has to be one of : raw, prop, extvp, big");
				System.exit(1);
			}
		} catch (SQLException e) {
			System.err.println("[ERROR] SQL exception: " + e.getLocalizedMessage());
			System.exit(1);
		}
	}
}
