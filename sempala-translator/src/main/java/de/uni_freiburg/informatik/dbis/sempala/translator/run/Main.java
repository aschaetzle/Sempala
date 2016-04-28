package de.uni_freiburg.informatik.dbis.sempala.translator.run;

import java.io.File;

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
 * @author Antony Neu
 */
public class Main {

	private static String input;
	private static String output;

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
			logger.fatal(e.getMessage(), e);
			System.exit(-1);
		}

		if (commandLine.hasOption(OptionNames.HELP.toString())) {
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("SparqlEvaluator", options, true);
		}


		/*
		 *  Set properties of translator
		 */

		Translator translator = new Translator();

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
			System.err.println("Fatal: Invalid format specified.");
			System.exit(1);
		}

		// No check, input is required
		input = commandLine.getOptionValue(OptionNames.INPUT.toString());

		// If output is not specified use input as output
		if ( commandLine.hasOption(OptionNames.OUTPUT.toString()) )
			output = commandLine.getOptionValue(OptionNames.OUTPUT.toString());
		else
			output = commandLine.getOptionValue(OptionNames.INPUT.toString());


		/*
		 *  Run translator
		 */

		File inputFile = new File(input);
		if (inputFile.isDirectory()){

			// Run the translator for every file in the folder that matches the common sparql extensions
			for(final File fileEntry : inputFile.listFiles()){
				if(fileEntry.getName().matches("(.*\\.sq|.*\\.srx|.*\\.sparql)$")) { // Match only SPARQL extensions
					System.out.println("Translating file " + fileEntry.getName());
					translator.setInputFile(fileEntry.getAbsolutePath());
					translator.setOutputFile(fileEntry.getAbsolutePath());
					translator.translateQuery();
				}
			}

		} else {

			// Run the translator the for the given in- and output
			translator.setInputFile(input);
			translator.setOutputFile(output);
			translator.translateQuery();

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
		FORMAT,
		HELP,
		INPUT,
		OUTPUT,
		OPTIMIZE;

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
				Option.builder("o")
				.longOpt(OptionNames.OUTPUT.toString())
				.hasArg()
				.argName("path")
				.desc("Imapala output script file")
				.build());

		options.addOption(
				Option.builder("opt")
				.longOpt(OptionNames.OPTIMIZE.toString())
				.desc("turn on SPARQL algebra optimization")
				.build());

		return options;
	}


}
