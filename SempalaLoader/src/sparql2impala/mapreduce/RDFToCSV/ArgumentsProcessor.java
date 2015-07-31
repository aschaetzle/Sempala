package sparql2impala.mapreduce.RDFToCSV;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Processes all command line flags and arguments.
 * 
 * @author Antony Neu
 * 
 */
public class ArgumentsProcessor {

	private CommandLine cli;

	private boolean logging;
	private boolean lubm = false;

	public boolean isLubm() {
		return lubm;
	}

	public boolean isLogging() {
		return logging;
	}

	private String logFile;

	public String getLogFile() {
		return logFile;
	}

	private TableFormat format;

	public TableFormat getFormat() {
		return format;
	}

	private boolean nquads = false;

	public boolean isNquads() {
		return nquads;
	}

	Options options = new Options();

	/**
	 * Process command line options and arguments.
	 */
	public ArgumentsProcessor(String[] args) {
		options.addOption("l", "enable-logging", false,
				"Activates/Deactivates all logging.");
		options.addOption("lf", "logfile", true,
				"Log not only to standard output but to file.");
		options.addOption("f", "format", true,
				"Decide which format: \n0: Vertical Partitioning \n1: Triple Store \n2: Property Table \n3: Partitioned Property Table");
		options.addOption("nq", "n-quads", false,
				"Parses nquads by only taking first three elements.");
		options.addOption("lu", "lubm", false,
				"Enable LUBM dataset preprocessing");

		CommandLineParser parser = new GnuParser();
		try {
			cli = parser.parse(options, args);
		} catch (ParseException e) {
			outputUsage();
			System.exit(2);
		}

		// format must be specified option
		if ((cli.hasOption("f") && cli.getOptionValue("f") != null)
				|| (cli.hasOption("format") && cli.getOptionValue("format") != null)) {
			String form = "";
			if (cli.hasOption("f") && cli.getOptionValue("f") != null) {
				form = cli.getOptionValue("f");
			} else if (cli.hasOption("format")
					&& cli.getOptionValue("format") != null) {
				form = cli.getOptionValue("format");
			}
			System.out.println("Format is " + form);

			if (form.equals("0")) {
				this.format = TableFormat.VERTICAL;
			} else if (form.equals("1")) {
				this.format = TableFormat.TRIPLESTORE;
			} else if (form.equals("3")) {
				this.format = TableFormat.PARTITIONED_BIGTABLE;
			} else {
				this.format = TableFormat.WIDETABLE;
			}

			// log to log file?
			logFile = "log.txt";
			if (cli.hasOption("lf") || cli.hasOption("logfile")) {
				if (cli.getOptionValue("lf") != null) {
					logFile = cli.getOptionValue("lf");
				} else if (cli.getOptionValue("logfile") != null) {
					logFile = cli.getOptionValue("logfile");
				} else {
					System.out.println("Specify log file. Logging to log.txt");
				}
			}

			// logfile not neccessary
			if (cli.getOptionValue("lf") != null) {
				logFile = cli.getOptionValue("lf");
			} else if (cli.getOptionValue("logfile") != null) {
				logFile = cli.getOptionValue("logfile");
			} else {
				System.out.println("Specify log file. Logging to log.txt");
			}

			// lubm?
			lubm = cli.hasOption("lu") || cli.hasOption("lubm");

		}
	}

	/**
	 * Automatically generate the help statement
	 */
	public void outputUsage() {

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("RDFConverter", options);
	}

	public String[] getOtherArgs() {
		return cli.getArgs();
	}

}
