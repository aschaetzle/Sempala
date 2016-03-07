package de.uni_freiburg.informatik.dbis.sempala.loader.run;

import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_freiburg.informatik.dbis.sempala.loader.Loader;
import de.uni_freiburg.informatik.dbis.sempala.loader.PropertyTableLoader;
import de.uni_freiburg.informatik.dbis.sempala.loader.SingleTableLoader;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.Impala;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.Impala.QueryOption;

/**
 * 
 * @author Manuel Schneider <schneidm@informatik.uni-freiburg.de>
 *
 */
public class Main {
	
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
		DATABASE,
		FORMAT,
		FIELD_TERMINATOR,
		HELP,
		HOST,
		INPUT,
		KEEP,
		LINE_TERMINATOR,
		OUTPUT,
		PORT,
		PREFIX_FILE,
		STRIP_DOT,
		SHUFFLE,
		UNIQUE;
	
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
		
		// Get the required options
		String host = commandLine.getOptionValue(OptionNames.HOST.toString());
		String port = commandLine.getOptionValue(OptionNames.PORT.toString());
		String database = commandLine.getOptionValue(OptionNames.DATABASE.toString());
		String hdfsInputDirectory = commandLine.getOptionValue(OptionNames.INPUT.toString());
		String tablenameOutput = commandLine.getOptionValue(OptionNames.OUTPUT.toString());

		// Print help if requested
		if (commandLine.hasOption(OptionNames.HELP.toString()))
			printHelpAndExit(0);

		// Connect to impalad
		Impala impala = null;
		try {
			impala = new Impala(host, port, database);
			
			// Set compression codec to snappy
			impala.set(QueryOption.COMPRESSION_CODEC, "SNAPPY");
		} catch (SQLException e) {
			System.err.println("Fatal: Could not connect to impalad: " + e.getLocalizedMessage());
			System.exit(1);
		}
		
		// Construct the loader corresponding to format
		Loader loader = null;
		String format = commandLine.getOptionValue(OptionNames.FORMAT.toString());
		if (format.equals(Format.PROPERTYTABLE.toString()))
			loader = new PropertyTableLoader(impala, hdfsInputDirectory, tablenameOutput);
//		else if (format.equals(Format.EXTVP.toString())
//			throw new NotImplementedException("Extended vertical partitioning is not implemented yet");
		else if (format.equals(Format.SINGLETABLE.toString()))
			loader = new SingleTableLoader(impala, hdfsInputDirectory, tablenameOutput);
		else {
			System.err.println("Fatal: Invalid format.");
			printHelpAndExit(1);
		}
				
		// Set the options of the loader
		if(commandLine.hasOption(OptionNames.COLUMN_NAME_SUBJECT.toString()))
			loader.column_name_subject = commandLine.getOptionValue(OptionNames.COLUMN_NAME_SUBJECT.toString());
		
		if(commandLine.hasOption(OptionNames.COLUMN_NAME_PREDICATE.toString()))
			loader.column_name_predicate = commandLine.getOptionValue(OptionNames.COLUMN_NAME_PREDICATE.toString());

		if(commandLine.hasOption(OptionNames.COLUMN_NAME_OBJECT.toString()))
			loader.column_name_object = commandLine.getOptionValue(OptionNames.COLUMN_NAME_OBJECT.toString());

		if(commandLine.hasOption(OptionNames.FIELD_TERMINATOR.toString()))
			loader.field_terminator = commandLine.getOptionValue(OptionNames.FIELD_TERMINATOR.toString());

		if(commandLine.hasOption(OptionNames.KEEP.toString()))
			loader.keep = commandLine.hasOption(OptionNames.KEEP.toString());

		if(commandLine.hasOption(OptionNames.LINE_TERMINATOR.toString()))
			loader.line_terminator = commandLine.getOptionValue(OptionNames.LINE_TERMINATOR.toString());

		if(commandLine.hasOption(OptionNames.PREFIX_FILE.toString()))
			loader.prefix_file = commandLine.getOptionValue(OptionNames.PREFIX_FILE.toString());

		if(commandLine.hasOption(OptionNames.STRIP_DOT.toString()))
			loader.strip_dot = commandLine.hasOption(OptionNames.STRIP_DOT.toString());

		if(commandLine.hasOption(OptionNames.SHUFFLE.toString()))
			loader.shuffle = commandLine.hasOption(OptionNames.SHUFFLE.toString());

		if(commandLine.hasOption(OptionNames.UNIQUE.toString()))
			loader.unique = commandLine.hasOption(OptionNames.UNIQUE.toString());

		// Eventually build the table
		try {
			loader.load();
		} catch (SQLException e) {
			System.err.println("Fatal: SQL exception: " + e.getLocalizedMessage());
			System.exit(1);
		}
	}	
	
	/**
	 * Prints the commons cli help and exits with given return value
	 */
	private static void  printHelpAndExit(int exitValue) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("java -jar <path to jar> loader [options]", buildOptions());
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
				Option.builder("d")
				.longOpt(OptionNames.DATABASE.toString())
				.desc("The database to use.")
				.hasArg()
				.required()
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
				Option.builder("F")
				.longOpt(OptionNames.FIELD_TERMINATOR.toString())
				.desc("The character used to separate the fields in the data. (Defaults to '\\t')")
				.hasArg()
				.build()
				);

		options.addOption(
				Option.builder("h")
				.longOpt(OptionNames.HELP.toString())
				.desc("Print this help.")
				.hasArg()
				.build());

		options.addOption(
				Option.builder("H")
				.longOpt(OptionNames.HOST.toString())
				.desc("The host to connect to.")
				.hasArg()
				.required()
				.build());

		options.addOption(
				Option.builder("i")
				.longOpt(OptionNames.INPUT.toString())
				.desc("The HDFS location of the RDF data (N-Triples).")
				.hasArg()
				.required()
				.build()
				);

		options.addOption(
				Option.builder("k")
				.longOpt(OptionNames.KEEP.toString())
				.desc("Do not drop temporary tables.")
				.build());

		options.addOption(
				Option.builder("L")
				.longOpt(OptionNames.LINE_TERMINATOR.toString())
				.desc("The character used to separate the lines in the data. (Defaults to '\\n')")
				.hasArg()
				.build());

		options.addOption(
				Option.builder("o")
				.longOpt(OptionNames.OUTPUT.toString())
				.desc("The name of the table to create.")
				.hasArg()
				.required()
				.build());

		options.addOption(
				Option.builder("p")
				.longOpt(OptionNames.PORT.toString())
				.desc("The port to connect to. (Defaults to "+Impala.defaultPort+")")
				.hasArg()
				.build());

		options.addOption(
				Option.builder("P")
				.longOpt(OptionNames.PREFIX_FILE.toString())
				.desc("The prefix file in TURTLE format.\nUsed to replace namespaces by prefixes.")
				.hasArg()
				.build()
				);

		options.addOption(
				Option.builder("s")
				.longOpt(OptionNames.STRIP_DOT.toString())
				.desc("Strip the dot in the last field (N-Triples)")
				.build());

		options.addOption(
				Option.builder("S")
				.longOpt(OptionNames.SHUFFLE.toString())
				.desc("Use shuffle strategy for join operations")
				.build());

		options.addOption(
				Option.builder("u")
				.longOpt(OptionNames.UNIQUE.toString())
				.desc("Detect and ignore duplicates in the input (Memoryintensive!)")
				.build());
		
		return options;
	}
}
