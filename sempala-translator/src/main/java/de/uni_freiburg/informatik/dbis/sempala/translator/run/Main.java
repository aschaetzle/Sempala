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
 * @author Antony Neu
 */
public class Main {

	private static String inputFile;
	private static String outputFile;
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
		
		if (commandLine.hasOption(OptionNames.OPTIMIZE.toString())) {
			optimize = true;
			logger.info("SPARQL Algebra optimization is turned on");
		}
		
		if (commandLine.hasOption(OptionNames.EXPAND.toString())) {
			expand = true;
			logger.info("URI prefix expansion is turned on");
		}
		
		if (commandLine.hasOption(OptionNames.INPUT.toString())) {
			inputFile = commandLine.getOptionValue(OptionNames.INPUT.toString());
		}
		
		if (commandLine.hasOption(OptionNames.OUTPUT.toString())) {
			outputFile = commandLine.getOptionValue(OptionNames.OUTPUT.toString());
		} else {
			outputFile = commandLine.getOptionValue(OptionNames.INPUT.toString());
		}

		if (commandLine.hasOption(OptionNames.FOLDER.toString())) {
			folderName = commandLine.getOptionValue(OptionNames.FOLDER.toString());
		}
		
		
		/*
		 *  Run translator
		 */
			
		if(folderName.equals("")){
			// instantiate Translator
			Translator translator = new Translator(inputFile, outputFile);
			translator.setOptimizer(optimize);
			translator.setExpandPrefixes(expand);
			translator.translateQuery();
			
		} else {
			File folderfile = new File(folderName);
			for(final File fileEntry : folderfile.listFiles()){
				if(fileEntry.getName().contains("sparql") && !fileEntry.getName().contains("log") && !fileEntry.getName().contains("sql")){
					System.out.println("Tranlsating file "+fileEntry.getName());
					// instantiate Translator
					Translator translator = new Translator(fileEntry.getAbsolutePath(), fileEntry.getAbsolutePath());
					translator.setOptimizer(optimize);
					translator.setExpandPrefixes(expand);
					translator.translateQuery();
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
		FOLDER,
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
				.longOpt(OptionNames.FOLDER.toString())
				.hasArg()
				.argName("folder")
				.desc("Imapala output script file")
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
				.argName("file")
				.desc("SPARQL query file to translate")
				.required()
				.build());
		
		options.addOption(
				Option.builder("o")
				.longOpt(OptionNames.OUTPUT.toString())
				.hasArg()
				.argName("file")
				.desc("Imapala output script file")
				.required()
				.build());
		
		options.addOption(
				Option.builder("opt")
				.longOpt(OptionNames.OPTIMIZE.toString())
				.desc("turn on SPARQL algebra optimization")
				.build());
		
		return options;
	}
	
	
}
