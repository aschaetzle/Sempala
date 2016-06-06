package de.uni_freiburg.informatik.dbis.sempala.translator.run;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import de.uni_freiburg.informatik.dbis.sempala.translator.Format;
import de.uni_freiburg.informatik.dbis.sempala.translator.Translator;

/**
 * Main Class for program start. Parses the commandline arguments and calls the
 * Sempala translator.
 *
 * @author Antony Neu, Manuel Schneider
 */
public class Main {

	/** The input file/folder to write to */
	private static String inputPath = null;

	/** The connection to the impala daemon */
    private static Connection connection = null;

	// Define a static logger variable so that it references the corresponding
	// Logger instance
	private static final Logger logger = Logger.getLogger(Main.class);

	/**
	 * Main method invoked on program start. It parses the commandline arguments
	 * and calls the Translator.
	 *
	 * @param args
	 *            commandline arguments
	 */
	public static void main(String[] args) {

		Translator translator = new Translator();

		/*
		 *  Parse the command line
		 */

		Options options = buildOptions();
		CommandLineParser parser = new DefaultParser();
		CommandLine commandLine = null;
		try {
			commandLine = parser.parse(options, args);
		} catch (ParseException e) {
			// error when parsing commandline arguments
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("SparqlEvaluator", options, true);
			logger.fatal(e.getLocalizedMessage());
			System.exit(-1);
		}

		if (commandLine.hasOption(OptionNames.HELP.toString())) {
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("SparqlEvaluator", options, true);
		}

		// If host, port or database is defined, host and database are required
		if (commandLine.hasOption(OptionNames.HOST.toString())
				|| commandLine.hasOption(OptionNames.PORT.toString())
				|| commandLine.hasOption(OptionNames.DATABASE.toString())){
			if (commandLine.hasOption(OptionNames.HOST.toString())
					&& commandLine.hasOption(OptionNames.DATABASE.toString())){

				// Establish the connection to impalad
				String host = commandLine.getOptionValue(OptionNames.HOST.toString());
				String port = commandLine.getOptionValue(OptionNames.PORT.toString(), "21050");
				String database = commandLine.getOptionValue(OptionNames.DATABASE.toString());
				String impalad_url = String.format("jdbc:impala://%s:%s/%s", host, port, database);
				System.out.println(String.format("Connecting to impalad (%s)", impalad_url));
				try {
					connection = DriverManager.getConnection(impalad_url);
				} catch (SQLException e) {
					logger.fatal(e.getLocalizedMessage());
					System.exit(1);
				}

			} else {
				logger.fatal("If host, port or database is defined, host and database are required");
				System.exit(1);
			}
		}

		// Enable optimizations if requested
		if (commandLine.hasOption(OptionNames.OPTIMIZE.toString())) {
			translator.setOptimizer(true);
			logger.info("SPARQL Algebra optimization is turned on");
		}

		// Enable prefix expansion if requested
		if (commandLine.hasOption(OptionNames.EXPAND.toString())) {
			translator.setExpandPrefixes(true);
			logger.info("URI prefix expansion is turned on");
		}

		// Set requested format
		String format = commandLine.getOptionValue(OptionNames.FORMAT.toString());
		if (format.equals(Format.PROPERTYTABLE.toString())) {
			translator.setFormat(Format.PROPERTYTABLE);
			logger.info("Format set to propertytable.");
		} else if (format.equals(Format.SINGLETABLE.toString())) {
			translator.setFormat(Format.SINGLETABLE);
			logger.info("Format set to singletable.");
		} else {
			logger.fatal("Fatal: Invalid format specified.");
			System.exit(1);
		}

		// No check, input is required
		inputPath = commandLine.getOptionValue(OptionNames.INPUT.toString());

		/*
		 *  Run translator
		 */

		File inputFile = new File(inputPath);
		if ( !inputFile.exists() ){
			logger.fatal("Input path does not exist.");
			System.exit(1);
		}

		// Get a list of files that have to be handled
		List<String> inputPaths = new ArrayList<>();
		if (inputFile.isDirectory()){
			// Run the translator for every file in the folder that matches the common sparql extensions
			for(final File fileEntry : inputFile.listFiles()){
				if(fileEntry.getName().matches("(.*\\.sq|.*\\.srx|.*\\.sparql)$")) { // Match only SPARQL extensions
					inputPaths.add(fileEntry.getAbsolutePath());
				}
			}
		} else {
			inputPaths.add(inputFile.getAbsolutePath());
		}

		// Translate every file
		String sqlString = null;
		for ( final String filePath : inputPaths ){
			System.out.println("Translating file " + filePath);
			translator.setInputFile(filePath);
			sqlString = translator.translateQuery();

			if (connection != null) {
				try {
					connection.createStatement().executeUpdate(sqlString);
				} catch (SQLException e) {
					logger.warn("Querying database failed!", e);
				}
			} else {
				// Print resulting SQL script program to output file
				PrintWriter printWriter;
				try {
					printWriter = new PrintWriter(filePath + ".sql");
					printWriter.print(sqlString);
					printWriter.close();
				} catch (Exception e) {
					logger.warn("Cannot open output file: " + filePath + ".sql", e);
				}
			}
		}
	}

	/** An enumeration of the options supported by this loader
	 *
	 * -h, --help prints the usage help message
	 * -e, --expand expand prefixes used in the query
	 * -opt, --optimize turn on SPARQL algebra optimization
	 * -i, --input <file> SPARQL query file to translate
	 * -o, --output <file> Impala output script file
	 */
	private enum OptionNames {
		EXPAND,
		DATABASE,
		FORMAT,
		HELP,
		HOST,
		INPUT,
		OPTIMIZE,
		PORT;

		@Override
		public String toString() {
			return super.toString().toLowerCase();
		}
	}


	/**
	 * Builds the options for this application
	 *
	 * @return The options collection
	 */
	public static Options buildOptions() {

		Options options = new Options();

		options.addOption(
				Option.builder("e")
				.longOpt(OptionNames.EXPAND.toString())
				.desc("expand URI prefixes")
				.build());

		options.addOption(
				Option.builder("f")
				.longOpt(OptionNames.FORMAT.toString())
				.desc("The database format the query is built for.\n"
						+ Format.PROPERTYTABLE.toString() + ": (see 'Sempala: Interactive SPARQL Query Processing on Hadoop')\n"
						+ Format.SINGLETABLE.toString() + ": see ExtVP Bigtable, Master's Thesis: S2RDF, Skilevic Simon")
				.hasArg()
				.required()
				.build());

		options.addOption(
				Option.builder("h")
				.longOpt(OptionNames.HELP.toString())
				.desc("print this message")
				.build());

		options.addOption(
				Option.builder("i")
				.longOpt(OptionNames.INPUT.toString())
				.hasArg()
				.argName("path")
				.desc("SPARQL query file to translate or folder containing sparql query files.")
				.required()
				.build());

		options.addOption(
				Option.builder("opt")
				.longOpt(OptionNames.OPTIMIZE.toString())
				.desc("turn on SPARQL algebra optimization")
				.build());

		options.addOption(
				Option.builder("d")
				.longOpt(OptionNames.DATABASE.toString())
				.desc("The database to use.")
				.hasArg()
				.build());

		options.addOption(
				Option.builder("H")
				.longOpt(OptionNames.HOST.toString())
				.desc("The host to connect to.")
				.hasArg()
				.build());

		options.addOption(
				Option.builder("p")
				.longOpt(OptionNames.PORT.toString())
				.desc("The port to connect to. (Defaults to 21050)")
				.hasArg()
				.build());



		return options;
	}


}
