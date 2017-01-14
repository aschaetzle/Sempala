package de.uni_freiburg.informatik.dbis.sempala.loader.run;

import java.sql.SQLException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_freiburg.informatik.dbis.sempala.loader.ExtVPLoader;
import de.uni_freiburg.informatik.dbis.sempala.loader.Loader;
import de.uni_freiburg.informatik.dbis.sempala.loader.SimplePropertyTableLoader;
import de.uni_freiburg.informatik.dbis.sempala.loader.SingleTableLoader;
import de.uni_freiburg.informatik.dbis.sempala.loader.spark.ComplexPropertyTableLoader;
import de.uni_freiburg.informatik.dbis.sempala.loader.spark.Spark;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.Impala;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.Impala.QueryOption;

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
		CommandLineParser parser = new BasicParser();
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
		System.out.println(format);
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
			String master = commandLine.getOptionValue(OptionNames.MASTER.toString(), "local");
			spark = new Spark("sempalaApp", database, master);
		}


		/*
		 *  Setup loader
		 */

		Loader loader = null;
		ComplexPropertyTableLoader complexPropertyLoader = null;
		// Construct the loader corresponding to format
		String hdfsInputDirectory = commandLine.getOptionValue(OptionNames.INPUT.toString());
		if (format.equals(Format.SIMPLE_PROPERTY_TABLE.toString())) {
			loader = new SimplePropertyTableLoader(impala, hdfsInputDirectory);
		}
		else if (format.equals(Format.COMPLEX_PROPERTY_TABLE.toString())){
			complexPropertyLoader = new ComplexPropertyTableLoader(spark, hdfsInputDirectory);
		}
		else if (format.equals(Format.SINGLE_TABLE.toString())){
			loader = new SingleTableLoader(impala, hdfsInputDirectory);
		}
		else if (format.equals(Format.EXTVP.toString()))
			loader = new ExtVPLoader(impala, hdfsInputDirectory);		
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

		if (loader != null) {
			try {
				loader.load();
			} catch (SQLException e) {
				System.err.println("Fatal: SQL exception: " + e.getLocalizedMessage());
				System.exit(1);
			}
		} else if(format.equals(Format.COMPLEX_PROPERTY_TABLE.toString()) && complexPropertyLoader != null) {
			complexPropertyLoader.load();
		}
	}

	/** An enumeration of the data formats supported by this loader */
	private enum Format {
		SIMPLE_PROPERTY_TABLE,
		COMPLEX_PROPERTY_TABLE,
		EXTVP,
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
		EXTVP_TYPES,
		FORMAT,
		FIELD_TERMINATOR,
		HELP,
		HOST,
		INPUT,
		KEEP,
		MASTER,
		LINE_TERMINATOR,
		LIST_OF_PREDICATES,
		OUTPUT,
		PORT,
		PREFIX_FILE,
		STRIP_DOT,
		SHUFFLE,
		THRESHOLD,
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
		options.addOption("cs", OptionNames.COLUMN_NAME_SUBJECT.toString(), true, "Overwrites the column name to use. (subject)");

		options.addOption("cp", OptionNames.COLUMN_NAME_PREDICATE.toString(), true, "Overwrites the column name to use. (predicate)");
		
		options.addOption("co", OptionNames.COLUMN_NAME_OBJECT.toString(), true, "Overwrites the column name to use. (object)");
		
		options.addOption("d",OptionNames.DATABASE.toString(), true, "The database to use.");
		
		options.addOption("e",OptionNames.EXTVP_TYPES.toString(),true,"Formats of ExtVP to be computed. By default all four formats of ExtVP (SS/SO/OS/OO) are computed");

		options.addOption("f", OptionNames.FORMAT.toString(), true, "The format to use to create the table. (case insensitive)\n"
				+ Format.SIMPLE_PROPERTY_TABLE.toString() + ": (see 'Sempala: Interactive SPARQL Query Processing on Hadoop')\n"
				//TODO change this when the final paper is ready
				+ Format.COMPLEX_PROPERTY_TABLE.toString() + ": (see Polina and Matteo's Master project paper) \n"
				+ Format.EXTVP.toString() + ": see Extended Vertical Partitioning, Master's Thesis: S2RDF, Skilevic Simon\n"
				+ Format.SINGLE_TABLE.toString() + ": see ExtVP Bigtable, Master's Thesis: S2RDF, Skilevic Simon");

		options.addOption("F",OptionNames.FIELD_TERMINATOR.toString(), true, "The character used to separate the fields in the data. (Defaults to '\\t')");

		options.addOption("h", OptionNames.HELP.toString(), false, "Print this help.");

		options.addOption("H", OptionNames.HOST.toString(), true, "The host to connect to.");

		options.addOption("i", OptionNames.INPUT.toString(), true, "The HDFS location of the RDF data (N-Triples).");

		options.addOption("k", OptionNames.KEEP.toString(), false, "Do not drop temporary tables.");
		
		options.addOption("lp",OptionNames.LIST_OF_PREDICATES.toString(),true,"List of predicates over which the ExtVP tables will be created.");
		
		options.addOption("L", OptionNames.LINE_TERMINATOR.toString(), true, "The character used to separate the lines in the data. (Defaults to '\\n')");
		
		options.addOption("m", OptionNames.MASTER.toString(), true, "The link for the spark master (Defaults to local)");
		
		options.addOption("o", OptionNames.OUTPUT.toString(), true, "Overwrites the name of the output table.");

		options.addOption("p", OptionNames.PORT.toString(), true, "The port to connect to. (Defaults to 21050)");
	
		options.addOption("P", OptionNames.PREFIX_FILE.toString(), true, "The prefix file in TURTLE format.\nUsed to replace namespaces by prefixes.");

		options.addOption("s", OptionNames.STRIP_DOT.toString(), false, "Strip the dot in the last field (N-Triples)");

		options.addOption("S", OptionNames.SHUFFLE.toString(), false, "Use shuffle strategy for join operations");

		options.addOption("t", OptionNames.THRESHOLD.toString(), true, "Threshold of ExtVP if ExtVP format is selected. Default (SF=1)");
		
		options.addOption("u", OptionNames.UNIQUE.toString(), false,"Detect and ignore duplicates in the input (Memoryintensive!)");

		return options;
	}
}

