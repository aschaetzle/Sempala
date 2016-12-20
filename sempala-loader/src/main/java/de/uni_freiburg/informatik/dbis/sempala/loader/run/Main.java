package de.uni_freiburg.informatik.dbis.sempala.loader.run;

import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_freiburg.informatik.dbis.sempala.loader.ComplexPropertyTableLoader;
import de.uni_freiburg.informatik.dbis.sempala.loader.Loader;
import de.uni_freiburg.informatik.dbis.sempala.loader.SimplePropertyTableLoader;
import de.uni_freiburg.informatik.dbis.sempala.loader.SingleTableLoader;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.framework.Impala;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.framework.Impala.QueryOption;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.framework.Spark;

/**
 *
 * @author Manuel Schneider <schneidm@informatik.uni-freiburg.de>
 *
 */
public class Main {

	/**
	 * The main routine.
	 * @param args The arguments passed to the program
	 */
	public static void main(String[] args) {

		// Parse command line
		Options options = buildOptions();
		CommandLine commandLine = null;
		CommandLineParser parser = new DefaultParser();
		try {
			commandLine = parser.parse(options, args);
		} catch (ParseException e) {
			// Commons CLI is poorly designed and uses exceptions for missing
			// required options. Therefore we can not print help without throwing
			// an exception. We'll just print it on every exception.
			System.err.println(e.getLocalizedMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(100);
			formatter.printHelp("sempala-loader", options, true);
			System.exit(1);
		}

		// parse the format of the table that will be built
		String format = commandLine.getOptionValue(OptionNames.FORMAT.toString());
		String database = commandLine.getOptionValue(OptionNames.DATABASE.toString());
		Impala impala = null;
		Spark spark = null;
		if (format.equals(Format.SIMPLE_PROPERTY_TABLE.toString()) || format.equals(Format.SINGLE_TABLE.toString())) {
			// Connect to the impala daemon
			try {
				String host = commandLine.getOptionValue(OptionNames.HOST.toString());
				String port = commandLine.getOptionValue(OptionNames.PORT.toString(), "21050");
				impala = new Impala(host, port, database);

				// Set compression codec to snappy
				impala.set(QueryOption.COMPRESSION_CODEC, "SNAPPY");
			} catch (SQLException e) {
				System.err.println(e.getLocalizedMessage());
				System.exit(1);
			}
		} else if(format.equals(Format.COMPLEX_PROPERTY_TABLE.toString())) {
			// use spark 
			// TODO make spark name a parameter or think about sth else
			spark = new Spark("sempalaApp", database);
		}


		/*
		 *  Setup loader
		 */

		Loader loader = null;

		// Construct the loader corresponding to format
		String hdfsInputDirectory = commandLine.getOptionValue(OptionNames.INPUT.toString());
		if (format.equals(Format.SIMPLE_PROPERTY_TABLE.toString())) {
			loader = new SimplePropertyTableLoader(impala, hdfsInputDirectory);
		}
		else if (format.equals(Format.COMPLEX_PROPERTY_TABLE.toString())){
			loader = new ComplexPropertyTableLoader(spark, hdfsInputDirectory);
		}
		else if (format.equals(Format.SINGLE_TABLE.toString())){
			loader = new SingleTableLoader(impala, hdfsInputDirectory);
		}
//		else if (format.equals(Format.EXTVP.toString())
//		throw new NotImplementedException("Extended vertical partitioning is not implemented yet");
		else {
			System.err.println("Fatal: Invalid format.");
			System.exit(1);
		}

		// Set the options of the loader
		if(commandLine.hasOption(OptionNames.OUTPUT.toString()))
			loader.tablename_output = commandLine.getOptionValue(OptionNames.OUTPUT.toString());

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

		/*
		 *  Run loader
		 */

		try {
			loader.load();
		} catch (SQLException e) {
			System.err.println("Fatal: SQL exception: " + e.getLocalizedMessage());
			System.exit(1);
		}
	}

	/** An enumeration of the data formats supported by this loader */
	private enum Format {
		SIMPLE_PROPERTY_TABLE,
		COMPLEX_PROPERTY_TABLE,
//		EXTVP,
		SINGLE_TABLE,;

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
	 * Builds the options for this application
	 * @return The options collection
	 */
	public static Options buildOptions() {

		Options options = new Options();

		// Add all other options
		options.addOption(
				Option.builder("cs")
				.longOpt(OptionNames.COLUMN_NAME_SUBJECT.toString())
				.desc("Overwrites the column name to use. (subject)")
				.hasArg()
				.argName("name")
				.build());

		options.addOption(
				Option.builder("cp")
				.longOpt(OptionNames.COLUMN_NAME_PREDICATE.toString())
				.desc("Overwrites the column name to use. (predicate)")
				.hasArg()
				.argName("name")
				.build());

		options.addOption(
				Option.builder("co")
				.longOpt(OptionNames.COLUMN_NAME_OBJECT.toString())
				.desc("Overwrites the column name to use. (object)")
				.hasArg()
				.argName("name")
				.build());

		options.addOption(
				Option.builder("d")
				.longOpt(OptionNames.DATABASE.toString())
				.desc("The database to use.")
				.hasArg()
				.argName("databse")
				.required()
				.build());

		options.addOption(
				Option.builder("f")
				.longOpt(OptionNames.FORMAT.toString())
				.desc("The format to use to create the table. (case insensitive)\n"
						+ Format.SIMPLE_PROPERTY_TABLE.toString() + ": (see 'Sempala: Interactive SPARQL Query Processing on Hadoop')\n"
						//TODO change this when the final paper is ready
						+ Format.COMPLEX_PROPERTY_TABLE.toString() + ": (see Polina and Matteo's Master project paper) \n"
//						+ Format.EXTVP.toString() + ": (Not implemented) see Extended Vertical Partitioning, Master's Thesis: S2RDF, Skilevic Simon\n"
						+ Format.SINGLE_TABLE.toString() + ": see ExtVP Bigtable, Master's Thesis: S2RDF, Skilevic Simon")
				.hasArg()
				.argName("format")
				.required()
				.build());

		options.addOption(
				Option.builder("F")
				.longOpt(OptionNames.FIELD_TERMINATOR.toString())
				.desc("The character used to separate the fields in the data. (Defaults to '\\t')")
				.hasArg()
				.argName("sep")
				.build());

		options.addOption(
				Option.builder("h")
				.longOpt(OptionNames.HELP.toString())
				.desc("Print this help.")
				.build());

		options.addOption(
				Option.builder("H")
				.longOpt(OptionNames.HOST.toString())
				.desc("The host to connect to.")
				.hasArg()
				.argName("host")
				.required()
				.build());

		options.addOption(
				Option.builder("i")
				.longOpt(OptionNames.INPUT.toString())
				.desc("The HDFS location of the RDF data (N-Triples).")
				.hasArg()
				.argName("path")
				.required()
				.build());

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
				.argName("terminator")
				.build());

		options.addOption(
				Option.builder("o")
				.longOpt(OptionNames.OUTPUT.toString())
				.desc("Overwrites the name of the output table.")
				.hasArg()
				.argName("name")
				.build());

		options.addOption(
				Option.builder("p")
				.longOpt(OptionNames.PORT.toString())
				.desc("The port to connect to. (Defaults to 21050)")
				.hasArg()
				.argName("port")
				.build());

		options.addOption(
				Option.builder("P")
				.longOpt(OptionNames.PREFIX_FILE.toString())
				.desc("The prefix file in TURTLE format.\nUsed to replace namespaces by prefixes.")
				.hasArg()
				.argName("file")
				.build());

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
