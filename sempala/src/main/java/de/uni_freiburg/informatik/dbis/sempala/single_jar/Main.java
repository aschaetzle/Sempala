package de.uni_freiburg.informatik.dbis.sempala.single_jar;

import java.util.Arrays;

/**
 * 
 * @author Manuel Schneider <schneidm@informatik.uni-freiburg.de>
 *
 */
public class Main {

	public enum HighLevelCommands { LOADER, TRANSLATOR }

	/**
	 * The main routine.
	 *
	 * @param args The arguments passed to the program
	 */
	public static void main(String[] args) {
		// Check the high level command and redirect to the responsible handler
		if (args.length==0)
			printUsageAndExit(1);
		if (args[0].toLowerCase().equals(HighLevelCommands.LOADER.toString().toLowerCase())
				|| args[0].toLowerCase().equals("l"))
			de.uni_freiburg.informatik.dbis.sempala.loader.run.Main.main(Arrays.copyOfRange(args, 1, args.length));
		else if (args[0].toLowerCase().equals(HighLevelCommands.TRANSLATOR.toString().toLowerCase())
				|| args[0].toLowerCase().equals("t"))
			de.uni_freiburg.informatik.dbis.sempala.translator.run.Main.main(Arrays.copyOfRange(args, 1, args.length));
		else
			printUsageAndExit(1);
	}

	/**
	 * Prints the usage and exits with given return value
	 */
	private static void  printUsageAndExit(int exitValue) {
		System.out.println("usage: 'java -jar <path to jar> {loader|l|translator|t}'");
		System.exit(exitValue);
	}
}
