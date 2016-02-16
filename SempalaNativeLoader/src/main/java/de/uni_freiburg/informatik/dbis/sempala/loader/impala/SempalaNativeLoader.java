package de.uni_freiburg.informatik.dbis.sempala.loader.impala;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
import de.uni_freiburg.informatik.dbis.sempala.impala.QueryOption;
import de.uni_freiburg.informatik.dbis.sempala.impala.SelectStatement;

/**
 *
 * @author Manuel Schneider <schneidm@informatik.uni-freiburg.de>
 *
 */
public class SempalaNativeLoader {

	/** The name used for RDF subject columns */
	public static String column_name_subject = "subject";

	/** The name used for RDF predicate columns */
	public static String column_name_predicate = "predicate";

	/** The name used for RDF object columns */
	public static String column_name_object = "object";

	/** The impala wrapper */
	private static Impala impala;

	/** The separator of the fields in the rdf data */
	private static String field_terminator;

	/** The separator of the lines in the rdf data */
	private static String line_terminator;

	/** The location of the input data */
	private static String hdfs_input_directory;
	
	/** The table name of the triple table */
	private static String tablename_triple_table = "tripletable";
	
	/** The name of the output table */
	private static String tablename_output;

	/** Indicates if dot at the end of the line is to be stripped */
	private static boolean strip_dot;

	/** Indicates if duplicates in the input are to be ignored */
	private static boolean unique;

	/** Indicates if shuffle strategy should be used for join operations */
	private static boolean shuffle;

	/** Indicates if created tables will be removed in case of an exception */
	private static boolean rollback;

	/** Indicates if temporary tables must not dropped */
	private static boolean keep;

	/** The map containing the prefixes */
	private static Map<String, String> prefix_map = null;
	
	/* A set containing created tables, intended for rollback operations */
	private static Set<String> createdTables = new HashSet<String>();

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
	public static void buildTripleTable() throws SQLException {

		final String tablename_external_tripletable = "external_tripletable";

		// Import the table from hdfs into impala
		System.out.println(String.format("Creating external table '%s' from hdfs data", tablename_external_tripletable));
		CreateStatement cet = impala
				.createTable(tablename_external_tripletable)
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
		createdTables.add(tablename_external_tripletable);
		impala.computeStats(tablename_external_tripletable);

		// Create a new parquet table, partitioned by predicate");
		System.out.print(String.format("Creating internal partitioned table '%s' from '%s'", tablename_triple_table, tablename_external_tripletable));
		
		long timestamp = System.currentTimeMillis();
		impala
		.createTable(tablename_triple_table)
		.storedAs(FileFormat.PARQUET)
		.addColumnDefinition(column_name_subject, DataType.STRING)
		.addColumnDefinition(column_name_object, DataType.STRING)
		.addPartitionDefinition(column_name_predicate, DataType.STRING)
		.execute();
		createdTables.add(tablename_triple_table);

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
		ss.from(tablename_external_tripletable);

		// Now insert the data into the new table
		impala
		.insertOverwrite(tablename_triple_table)
		.addPartition(column_name_predicate)
		.selectStatement(ss)
		.execute();
		System.out.println(String.format(" [%.3fs]", (float)(System.currentTimeMillis() - timestamp)/1000));
		impala.computeStats(tablename_triple_table);
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
	public static String prefixHelper(String column_name) {
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
	 * Creates a new property table from a triple table.
	 *
	 * The input table has to be in triple table format. The output will be a
	 * table in format described in 'Sempala: Interactive SPARQL Query
	 * Processing on Hadoop'.
	 *
	 * @throws SQLException
	 */
	public static void buildPropertyTable() throws SQLException {

		final String tablename_distinct_subjects = "distinct_subjects";
		
		// Build a table containing distinct subjects
		System.out.print(String.format("Creating table containing distinct subjects (%s)", tablename_distinct_subjects));
		long timestamp = System.currentTimeMillis();
		impala
		.createTable(tablename_distinct_subjects)
		.storedAs(FileFormat.PARQUET)
		.asSelect(
				impala
				.select(column_name_subject)
				.distinct()
				.from(tablename_triple_table)
				)
		.execute();
		createdTables.add(tablename_distinct_subjects);
		System.out.println(String.format(" [%.3fs]", (float)(System.currentTimeMillis() - timestamp)/1000));
		impala.computeStats(tablename_distinct_subjects);

		System.out.print(String.format("Creating property table (%s)", tablename_output));
		timestamp = System.currentTimeMillis();
		
		// Get properties
		ResultSet resultSet = impala
				.select(column_name_predicate)
				.distinct()
				.from(tablename_triple_table)
				.execute();

		// Convert the result set to a list
		ArrayList<String> predicates = new ArrayList<String>();
		while (resultSet.next())
			predicates.add(resultSet.getString(column_name_predicate));

		// Build a select stmt for the Insert-as-select statement
		SelectStatement sstmt = impala.select();

		// Add the subject column
		sstmt.addProjection(String.format("subjects.%s", column_name_subject));

		// Add the property columns to select clause (", t<x>.object AS <predicate>")
   	    for (int i = 0; i < predicates.size(); i++)
			sstmt.addProjection(String.format("t%d.%s AS %s", i, column_name_object, toImpalaColumnName(predicates.get(i))));

		// Add distinct subjects table reference
		sstmt.from(String.format("%s subjects", tablename_distinct_subjects));

		// Append the properties via join
		// "LEFT JOIN <tablename_internal_parquet> t<x> ON (t1.subject =
		// t<x>.subject AND t<x>.predicate = <predicate>)" to from clause
		for (int i = 0; i < predicates.size(); i++)
   	    	sstmt.leftJoin(
   	    			String.format("%s t%d", tablename_triple_table, i),
   	    			String.format("subjects.%2$s = t%1$d.%2$s AND t%1$d.%3$s = '%4$s'",
   	    					i, column_name_subject,
   	    					column_name_predicate, predicates.get(i)),
   	    			shuffle);

		// Create the property table "s, p, o[, p1, ...]"
		CreateStatement cstmt = impala.createTable(tablename_output).ifNotExists();
		cstmt.addColumnDefinition(column_name_subject, DataType.STRING);
		for (String pred : predicates)
			cstmt.addColumnDefinition(toImpalaColumnName(pred), DataType.STRING);
		cstmt.storedAs(FileFormat.PARQUET);
		cstmt.execute();
		createdTables.add(tablename_output);
		
		// Insert data into the single table using the built select stmt
		impala
		.insertOverwrite(tablename_output)
		.selectStatement(sstmt)
		.execute();
		System.out.println(String.format(" [%.3fs]", (float)(System.currentTimeMillis() - timestamp)/1000));
		impala.computeStats(tablename_output);
	}
	
	/**
	 * @param hdfs_input_directory
	 *            The directory containing the RDF data
	 * @param tablename_output
	 *            The name of the impala table to create
	 * @throws SQLException
	 */
	public static void buildExtendedVerticalPartitioningTable()
			throws SQLException {
		// TODO
	}
	
	/**
	 * Creates a new singletable from a triple table.
	 *
	 * The input table has to be in triple table format. The output will be a
	 * table in format described in 'Master's Thesis: S2RDF, Skilevic Simon'
	 *
	 * @throws SQLException
	 */
	public static void buildSingleTable() throws SQLException  {

		final String tablename_distinct_sp_relations = "distinct_sp_relations";
		final String tablename_distinct_op_relations = "distinct_op_relations";

		/*
		 * Create a table for the distinct subject-predicate tuples (not partitioned)
		 */
		
		System.out.print(String.format("Creating table '%s'", tablename_distinct_sp_relations));
		long timestamp = System.currentTimeMillis();
		impala
		.createTable(tablename_distinct_sp_relations)
		.storedAs(FileFormat.PARQUET)
		.asSelect(
				impala
				.select(column_name_subject)
				.addProjection(column_name_predicate)
				.distinct()
				.from(tablename_triple_table))
		.execute();
		createdTables.add(tablename_distinct_sp_relations);
		System.out.println(String.format(" [%.3fs]", (float)(System.currentTimeMillis() - timestamp)/1000));
		impala.computeStats(tablename_distinct_sp_relations);

		/*
		 * Create a table for the distinct object-predicate tuples (not partitioned)
		 */

		System.out.print(String.format("Creating table '%s'", tablename_distinct_op_relations));
		timestamp = System.currentTimeMillis();
		impala
		.createTable(tablename_distinct_op_relations)
		.storedAs(FileFormat.PARQUET)
		.ifNotExists()
		.asSelect(
				impala
				.select(column_name_object)
				.addProjection(column_name_predicate)
				.distinct()
				.from(tablename_triple_table))
		.execute();
		createdTables.add(tablename_distinct_op_relations);
		System.out.println(String.format(" [%.3fs]", (float)(System.currentTimeMillis() - timestamp)/1000));
		impala.computeStats(tablename_distinct_op_relations);

		/*
		 * Create the single table
		 */
		System.out.println(String.format("Starting the iterative creation of the singletable '%s'", tablename_output));
		timestamp = System.currentTimeMillis();
		
		// Get a list of all predicates
		ArrayList<String> predicates = new ArrayList<String>();
		ResultSet resultSet = impala.select(column_name_predicate).distinct().from(tablename_triple_table).execute();
		while (resultSet.next())
			predicates.add(resultSet.getString(column_name_predicate));
		
		// Create the new single table "s, p, o, [ss_p1, so_p1, os_p1], ..."
		CreateStatement cstmt = impala
				.createTable(tablename_output)
				.addColumnDefinition(column_name_subject, DataType.STRING)
				.addPartitionDefinition(column_name_predicate, DataType.STRING)
				.addColumnDefinition(column_name_object, DataType.STRING);
		for (String pred : predicates){
			String impalaConformPred = toImpalaColumnName(pred);
			cstmt.addColumnDefinition(String.format("ss_%s", impalaConformPred), DataType.BOOLEAN);
			cstmt.addColumnDefinition(String.format("so_%s", impalaConformPred), DataType.BOOLEAN);
			cstmt.addColumnDefinition(String.format("os_%s", impalaConformPred), DataType.BOOLEAN);
		}
		cstmt.storedAs(FileFormat.PARQUET);
		cstmt.execute();
		createdTables.add(tablename_output);

		/*
		 * Fill the single table
		 */

		// Sets containing existing relations  
		HashSet<String> SS_relations = new HashSet<String>();
		HashSet<String> SO_relations = new HashSet<String>();
		HashSet<String> OS_relations = new HashSet<String>();
		
		for (String predicate : predicates){

			System.out.print(String.format("Processing '%s'", predicate));
			long localtimestamp = System.currentTimeMillis();
			
			// Reset existing relations 
			SS_relations.clear(); 
			SO_relations.clear(); 
			OS_relations.clear();
			
			// Get all predicates that are in a SS relation to any triples in this partition (predicate)
			resultSet =
					impala
					.select(column_name_predicate).distinct()
					.from(String.format("%s sp", tablename_distinct_sp_relations))
					.leftSemiJoin(
							String.format("%s tt", tablename_triple_table),
							String.format("tt.%s=sp.%s AND tt.%s='%s'",
									column_name_subject, column_name_subject, column_name_predicate, predicate),
							shuffle)
					.execute();
			while (resultSet.next())
				SS_relations.add(resultSet.getString(column_name_predicate));
			
    		// Get all predicates that are in a SO relation to any triples in this partition (predicate)
			resultSet = impala.select(column_name_predicate).distinct()
					.from(String.format("%s op", tablename_distinct_op_relations))
					.leftSemiJoin(
							String.format("%s tt", tablename_triple_table),
							String.format("tt.%s=op.%s AND tt.%s='%s'",
									column_name_subject, column_name_object, column_name_predicate, predicate),
							shuffle)
					.execute();
			while (resultSet.next())
				SO_relations.add(resultSet.getString(column_name_predicate));
			
			// Get all predicates that are in a OS relation to any triples in this partition (predicate)
			resultSet =
					impala
					.select(column_name_predicate).distinct()
					.from(String.format("%s sp", tablename_distinct_sp_relations))
					.leftSemiJoin(
							String.format("%s tt", tablename_triple_table),
							String.format("tt.%s=sp.%s AND tt.%s='%s'",
									column_name_object, column_name_subject, column_name_predicate, predicate),
							shuffle)
					.execute();
			while (resultSet.next())
				OS_relations.add(resultSet.getString(column_name_predicate));

			// Build a select stmt for the Insert-as-select statement
			SelectStatement ss = impala.select();

			// Build the huge select clause
			ss.addProjection(String.format("tt.%s", column_name_subject));
			ss.addProjection(String.format("tt.%s", column_name_object));
			ss.from(String.format("%s tt", tablename_triple_table));
			
			for (String p : predicates){
				String impalaConformPredicate = toImpalaColumnName(p);
				
				// SS_p_i
				if (SS_relations.contains(p)){
					ss.addProjection(String.format("CASE WHEN %s.%s IS NULL THEN false ELSE true END AS %s",
							String.format("tss_%s", impalaConformPredicate),
							column_name_subject,
							String.format("ss_%s", impalaConformPredicate)));
					ss.leftJoin(
							// Table reference e.g. "LEFT JOIN subjects tss_p1"
							String.format("%s tss_%s", tablename_distinct_sp_relations, impalaConformPredicate),
							// On clause e.g. "ON tt.id=tss_p1.id AND tss_p1.predicate='wsdbm:friendOf'"
							String.format("tt.%1$s=tss_%3$s.%1$s AND tss_%3$s.%2$s='%4$s'",
									column_name_subject,
									column_name_predicate,
									impalaConformPredicate,
									p),
		   	    			shuffle);
				} else {
					ss.addProjection("false");
				}

				// SO_pi
				if (SO_relations.contains(p)){
					ss.addProjection(String.format("CASE WHEN %s.%s IS NULL THEN false ELSE true END AS %s",
							String.format("tso_%s", impalaConformPredicate),
							column_name_object,
							String.format("so_%s", impalaConformPredicate)));
					ss.leftJoin(
							// Table reference e.g. "LEFT JOIN objects tso_p1"
							String.format("%s tso_%s", tablename_distinct_op_relations, impalaConformPredicate),
							// On clause e.g. "ON tt.id=tso_p1.object AND tso_p1.predicate='wsdbm:friendOf'"
							String.format("tt.%1$s=tso_%4$s.%3$s AND tso_%4$s.%2$s='%5$s'",
									column_name_subject,
									column_name_predicate,
									column_name_object,
									impalaConformPredicate,
									p),
		   	    			shuffle);
				} else {
					ss.addProjection("false");
				}
				
				if (OS_relations.contains(p)){
					ss.addProjection(String.format("CASE WHEN %s.%s IS NULL THEN false ELSE true END AS %s",
							String.format("tos_%s", impalaConformPredicate),
							column_name_subject,
							String.format("os_%s", impalaConformPredicate)));
					ss.leftJoin(
							// Table reference e.g. "LEFT JOIN subjects tos_p1"
							String.format("%s tos_%s", tablename_distinct_sp_relations, impalaConformPredicate),
							// On clause e.g. "ON tt.object=tos_p1.id AND tos_p1.predicate='wsdbm:friendOf'"
							String.format("tt.%3$s=tos_%4$s.%1$s AND tos_%4$s.%2$s='%5$s'",
									column_name_subject,
									column_name_predicate,
									column_name_object,
									impalaConformPredicate,
									p),
		   	    			shuffle);
					
				} else {
					ss.addProjection("false");
				}
			}
			// Partition column at last (impala requirement)
			ss.addProjection(String.format("tt.%s", column_name_predicate));
			ss.where(String.format("tt.%s='%s'", column_name_predicate, predicate));
			
			// Insert data into the single table using the built select stmt
			impala
			.insertInto(tablename_output)
			.addPartition(column_name_predicate)
			.selectStatement(ss)
			.execute();
			
			System.out.println(String.format(" [%.3fs]", (float)(System.currentTimeMillis() - localtimestamp)/1000));
		}
		System.out.println(String.format("Singletable created in [%.3fs]", (float)(System.currentTimeMillis() - timestamp)/1000));
		impala.computeStats(tablename_output);
	}

	/**
	 * Makes the string conform to the requirements for impala column names.
	 * I.e. remove braces, replace non word characters, trim spaces.
	 * @param The string to make impala conform
	 * @return The impala conform string
	 */
	public static String toImpalaColumnName(String s) {
		// Space is a nonword character so trim before replacing chars.
		return s.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
	}

	/**
	 * Builds the options for this application
	 *
	 * @return The options collection
	 */
	public static Options buildOptions() {

		Options options = new Options();

		options.addOption(
				Option.builder("cs")
				.longOpt("column-name-subject")
				.desc("Overwrites the column name to use. (subject)")
				.hasArg()
				.build());

		options.addOption(
				Option.builder("cp")
				.longOpt("column-name-predicate")
				.desc("Overwrites the column name to use. (predicate)")
				.hasArg()
				.build());

		options.addOption(
				Option.builder("co")
				.longOpt("column-name-object")
				.desc("Overwrites the column name to use. (object)")
				.hasArg()
				.build());

		options.addOption(
				Option.builder("d")
				.longOpt("database")
				.desc("The database to use.")
				.hasArg()
				.required()
				.build());

		options.addOption(
				Option.builder("f")
				.longOpt("format")
				.desc("The format to use to create the table.\n"
						+ "prop : Propertytable (see 'Sempala: Interactive SPARQL Query Processing on Hadoop')\n"
						+ "extvp : (Not implemented)\n" //Extended Vertical Partitioning (see Master's Thesis: S2RDF, Skilevic Simon\n"
						+ "single : Singletable")
				.hasArg()
				.required()
				.build());

		options.addOption(
				Option.builder("F")
				.longOpt("field-terminator")
				.desc("The character used to separate the fields in the data. (Defaults to '\\t')")
				.hasArg()
				.required(false)
				.build()
				);

		options.addOption(
				Option.builder("h")
				.longOpt("host")
				.desc("The host to connect to.")
				.hasArg()
				.required()
				.build());

		options.addOption(
				Option.builder("i")
				.longOpt("input")
				.desc("The HDFS location of the RDF data (N-Triples).")
				.hasArg()
				.build()
				);

		options.addOption(
				Option.builder("I")
				.longOpt("input-table")
				.desc("If --input is not set, this option sets the table that is used to build sempala tables.\n"
						+ "If --input is set, this option sets the name of the intermediate triple table.\n"
						+ "If this option is omitted the name defaults to 'tripletable'.")
				.hasArg()
				.build()
				);

		options.addOption(
				Option.builder("k")
				.longOpt("keep-intermediate")
				.desc("Do not drop temporary tables.")
				.required(false)
				.build());

		options.addOption(
				Option.builder("L")
				.longOpt("line-terminator")
				.desc("The character used to separate the lines in the data. (Defaults to '\\n')")
				.hasArg()
				.required(false)
				.build());

		options.addOption(
				Option.builder("o")
				.longOpt("output")
				.desc("The name of the table to create.")
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
				Option.builder("P")
				.longOpt("prefix-file")
				.desc("The prefix file in TURTLE format.\nUsed to replace namespaces by prefixes.")
				.hasArg()
				.required(false)
				.build()
				);

		options.addOption(
				Option.builder("r")
				.longOpt("rollback")
				.desc("Drop all created tables in case of an SQL exception.")
				.required(false)
				.build()
				);

		options.addOption(
				Option.builder("s")
				.longOpt("strip-dot")
				.desc("Strip th dot in the last field (N-Triples)")
				.required(false)
				.build());

		options.addOption(
				Option.builder("S")
				.longOpt("shuffle")
				.desc("Use shuffle strategy for join operations")
				.required(false)
				.build());

		options.addOption(
				Option.builder("u")
				.longOpt("unique")
				.desc("Detect and ignore duplicates in the input (Memoryintensive!)")
				.required(false)
				.build());
		
		return options;
	}
	
	/**
	 * The main routine.
	 *
	 * @param args The arguments passed to the program
	 */
	public static void main(String[] args) {

		// Parse the command line
		CommandLine commandLine = null;
		Options options = buildOptions();
		try {
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("[ERROR] Parsing failed. Reason: " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("SempalaNativeLoader", options);
			System.exit(1);
		}

		if ( !(commandLine.hasOption("input") || commandLine.hasOption("input-table")) ){
			System.err.println("[ERROR] At least one of '--input' or '--input-table' has to be set.");
			System.exit(1);
		}

		String host = commandLine.getOptionValue("host");
		String port = commandLine.getOptionValue("port", "21050");
		String database = commandLine.getOptionValue("database");
		String format = commandLine.getOptionValue("format");
		column_name_subject = commandLine.getOptionValue("column-name-subject", column_name_subject);
		column_name_predicate = commandLine.getOptionValue("column-name-predicate", column_name_predicate);
		column_name_object = commandLine.getOptionValue("column-name-object", column_name_object);
		hdfs_input_directory = commandLine.getOptionValue("input");
		tablename_triple_table = commandLine.getOptionValue("input-table", tablename_triple_table);
		tablename_output = commandLine.getOptionValue("output");
		field_terminator = commandLine.getOptionValue("field-terminator", "\\t");
		line_terminator = commandLine.getOptionValue("line-terminator", "\\n");
		strip_dot = commandLine.hasOption("strip-dot");
		unique = commandLine.hasOption("unique");
		shuffle = commandLine.hasOption("shuffle");
		rollback = commandLine.hasOption("rollback");
		keep = commandLine.hasOption("keep-intermediate");

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
		
		try {
			// Connect to impalad
			impala = new Impala(host, port, database);

			// Set compression codec to snappy
			impala.set(QueryOption.COMPRESSION_CODEC, "SNAPPY");
			
			// If HDFS options is specified create a table in triple format from external data
			if (hdfs_input_directory != null)
				buildTripleTable();

			// Build the table in the requested format
			switch (format.toLowerCase()) {
			case "prop":
				buildPropertyTable();
				createdTables.remove(tablename_output);
				break;
			case "extvp":
				buildExtendedVerticalPartitioningTable();
				break;
			case "single":
				buildSingleTable();
				break;
			default:
				System.err.println("[ERROR] Invalid format.");
				System.exit(1);
			}

			// If intermediate tables should not be kept drop them
			if(!keep) {
				createdTables.remove(tablename_output);
				for (String tablename : createdTables)
					impala.dropTable(tablename);
			}
			
		} catch (SQLException e) {
			System.err.println("[ERROR] SQL exception: " + e.getLocalizedMessage());

			// Drop intermediate tables
			if (rollback){
				System.err.println("! ! ! Trying to roll back the operations ! ! !");
				for (String tablename : createdTables){
					try {
						impala.dropTable(tablename);
					} catch (SQLException f) {
						System.err.println("[FATAL] Another SQL exception: " + e.getLocalizedMessage());
						System.err.println("[FATAL] ! ! ! Could not roll back after exception ! ! !");
						System.exit(1);
					}
				}
			}
			System.exit(1);
		}
	}
}
