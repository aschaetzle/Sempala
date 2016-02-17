package de.uni_freiburg.informatik.dbis.sempala.translator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.NotImplementedException;

/**
 * 
 * @author Manuel Schneider <schneidm@informatik.uni-freiburg.de>
 *
 */
public class TranslatorMain {
	
	/** An enumeration of the data formats supported by this loader */
	private enum Format {
		PROPERTYTABLE,
//		EXTVP,
		SINGLETABLE;
	
	    @Override
	    public String toString() {
	        return super.toString().toLowerCase();
	    }
	}

	/** An enumeration of the options supported by this loader */
	private enum OptionNames {
		COLUMN_NAME_SUBJECT,
		COLUMN_NAME_PREDICATE,
		COLUMN_NAME_OBJECT,
		FORMAT,
		HELP,
		OUTPUT;
	
	    @Override
	    public String toString() {
	        return super.toString().replace('_', '-').toLowerCase();
	    }
	}

	/**
	 * The main routine.
	 *
	 * @param args The arguments passed to the program
	 */
	public static void main(String[] args) {

		// Parse the command line
		Options options = buildOptions();
		CommandLine commandLine = null;
		CommandLineParser parser = new DefaultParser();
		try {
			commandLine = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("Fatal: Parsing failed. Reason: " + e.getMessage());
			printHelpAndExit(1);
		}
		System.out.println(commandLine.toString());
		throw new NotImplementedException("TranslatorMain is not implemented");
		
	}
	

	/**
	 * Prints the commons cli help and exits with given return value
	 */
	private static void  printHelpAndExit(int exitValue) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("java -jar <path to jar> translator [options]", buildOptions());
		System.exit(exitValue);
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
				.longOpt(OptionNames.COLUMN_NAME_SUBJECT.toString())
				.desc("Overwrites the column name to use. (subject)")
				.hasArg()
				.build());

		options.addOption(
				Option.builder("cp")
				.longOpt(OptionNames.COLUMN_NAME_PREDICATE.toString())
				.desc("Overwrites the column name to use. (predicate)")
				.hasArg()
				.build());

		options.addOption(
				Option.builder("co")
				.longOpt(OptionNames.COLUMN_NAME_OBJECT.toString())
				.desc("Overwrites the column name to use. (object)")
				.hasArg()
				.build());

		options.addOption(
				Option.builder("f")
				.longOpt(OptionNames.FORMAT.toString())
				.desc("The format to use to create the table. (case insensitive)\n"
						+ Format.PROPERTYTABLE.toString() + ": (see 'Sempala: Interactive SPARQL Query Processing on Hadoop')\n"
//						+ Format.EXTVP.toString() + ": (Not implemented) see Extended Vertical Partitioning, Master's Thesis: S2RDF, Skilevic Simon\n"
						+ Format.SINGLETABLE.toString() + ": see ExtVP Bigtable, Master's Thesis: S2RDF, Skilevic Simon")
				.hasArg()
				.required()
				.build());

		options.addOption(
				Option.builder("h")
				.longOpt(OptionNames.HELP.toString())
				.desc("Print this help.")
				.hasArg()
				.build());

		options.addOption(
				Option.builder("o")
				.longOpt(OptionNames.OUTPUT.toString())
				.desc("The name of the table to create.")
				.hasArg()
				.required()
				.build());
		
		return options;
	}
}
