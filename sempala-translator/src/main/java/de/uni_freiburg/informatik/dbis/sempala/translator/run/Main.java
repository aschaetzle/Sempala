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

import de.uni_freiburg.informatik.dbis.sempala.translator.Translator;

/**
 * Main Class for program start. Parses the commandline arguments and calls the
 * Sempala translator.
 * 
 * Options: -h, --help prints the usage help message -d, --delimiter <value>
 * delimiter used in RDF triples if not whitespace -e, --expand expand prefixes
 * used in the query -opt, --optimize turn on SPARQL algebra optimization -i,
 * --input <file> SPARQL query file to translate -o, --output <file> Impala output
 * script file
 * 
 * @author Antony Neu
 */
public class Main {

	private static String inputFile;
	private static String outputFile;
	private static String delimiter = " ";
	private static boolean optimize = false;
	private static boolean expand = false;
	private static String folderName = "";

	// Define a static logger variable so that it references the corresponding
	// Logger instance
	private static final Logger logger = Logger.getLogger(Main.class);

	/**
	 * Main method invoked on program start. It parses the commandline arguments
	 * and calls the Translator.
	 * 
	 * Options: -h, --help prints the usage help message -d, --delimiter <value>
	 * delimiter used in RDF triples if not whitespace -e, --expand expand
	 * prefixes used in the query -opt, --optimize turn on SPARQL algebra
	 * optimization -i, --input <file> SPARQL query file to translate -o,
	 * --output <file> Impala output script file
	 * 
	 * @param args
	 *            commandline arguments
	 */
	public static void main(String[] args) {
		// parse the commandline arguments
		parseInput(args);
		
		if(!folderName.equals("")){
			File folderfile = new File(folderName);
			for(final File fileEntry : folderfile.listFiles()){
				if(fileEntry.getName().contains("sparql") && !fileEntry.getName().contains("log") && !fileEntry.getName().contains("sql")){
					System.out.println("Tranlsating file "+fileEntry.getName());
					// instantiate Translator
					Translator translator = new Translator(fileEntry.getAbsolutePath(), fileEntry.getAbsolutePath());
					translator.setOptimizer(optimize);
					translator.setExpandMode(expand);
					translator.setDelimiter(delimiter);
					translator.translateQuery();
				}
			}
			
		} else {
		// instantiate Translator
		Translator translator = new Translator(inputFile, outputFile);
		translator.setOptimizer(optimize);
		translator.setExpandMode(expand);
		translator.setDelimiter(delimiter);
		translator.translateQuery();
		}
	}

	/**
	 * Parses the commandline arguments.
	 * 
	 * @param args
	 *            commandline arguments
	 */
	private static void parseInput(String[] args) {
		// DEFINITION STAGE
		
		Options options = new Options();

		options.addOption(
				Option.builder("h")
				.longOpt("help")
				.desc("print this message")
				.build());
		
		options.addOption(
				Option.builder("opt")
				.longOpt("optimize")
				.desc("turn on SPARQL algebra optimization")
				.build());
		
		options.addOption(
				Option.builder("e")
				.longOpt("expand")
				.desc("expand URI prefixes")
				.build());
		
		options.addOption(
				Option.builder("d")
				.longOpt("delimiter")
				.hasArg()
				.argName("value")
				.desc("delimiter used in RDF triples if not whitespace")
				.required()
				.build());
		
		options.addOption(
				Option.builder("i")
				.longOpt("input")
				.hasArg()
				.argName("file")
				.desc("SPARQL query file to translate")
				.required()
				.build());
		
		options.addOption(
				Option.builder("o")
				.longOpt("output")
				.hasArg()
				.argName("file")
				.desc("Imapala output script file")
				.required()
				.build());
		
		options.addOption(
				Option.builder("f")
				.longOpt("folder")
				.hasArg()
				.argName("folder")
				.desc("Imapala output script file")
				.required()
				.build());

		
		// PARSING STAGE
		CommandLineParser parser = new  DefaultParser();
		CommandLine cmd = null;
		try {
			// parse the command line arguments
			cmd = parser.parse(options, args);
		} catch (ParseException exp) {
			// error when parsing commandline arguments
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("SparqlEvaluator", options, true);
			logger.fatal(exp.getMessage(), exp);
			System.exit(-1);
		}

		// INTERROGATION STAGE
		if (cmd.hasOption("help")) {
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("SparqlEvaluator", options, true);
		}
		if (cmd.hasOption("optimize")) {
			optimize = true;
			logger.info("SPARQL Algebra optimization is turned on");
		}
		if (cmd.hasOption("expand")) {
			expand = true;
			logger.info("URI prefix expansion is turned on");
		}
		if (cmd.hasOption("delimiter")) {
			delimiter = cmd.getOptionValue("delimiter");
			logger.info("Delimiter for RDF triples: " + delimiter);
		}
		if (cmd.hasOption("input")) {
			inputFile = cmd.getOptionValue("input");
		}
		if (cmd.hasOption("output")) {
			outputFile = cmd.getOptionValue("output");
		} else {
			outputFile = cmd.getOptionValue("input");
		}

		if (cmd.hasOption("folder")) {
			folderName = cmd.getOptionValue("folder");
		}
	}

}
