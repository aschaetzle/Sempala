package de.uni_freiburg.informatik.dbis;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


/**
 *
 * @author schneidm
 * 
 */
public class SempalaNativeLoader {
	
	static String JDBC_Driver = "com.cloudera.impala.jdbc41.Driver";
	
	public static void main(String[] args) throws Exception {
	
	    String hdfs_input;
	    String prefixfile;
	    String host;
	    int    port;
	    String database;
	    String bigtable_tablename;
		
	    
	    /*
	     * Create the options
	     */
	    
		Options options = new Options();
		
		options.addOption(
				Option.builder("i")
				.longOpt("input")
				.desc("The HDFS location of the RDF data (N-Triples).")
				.hasArg()
				.required()
				.build());
		
		options.addOption(
				Option.builder("pf")
				.longOpt("prefixfile")
				.desc("The prefixfile in TURTLE format.\nUsed to replace namespaces by prefixes.")
				.hasArg()
				.required(false)
				.build());
		
		options.addOption(
				Option.builder("h")
				.longOpt("host")
				.desc("The host to connect to.")
				.hasArg()
				.required()
				.build());
		
		options.addOption(
				Option.builder("p")
				.longOpt("port")
				.desc("The port to connect to. (Defaults to 21000)")
				.hasArg()
				.required(false)
				.build());
		
		options.addOption(
				Option.builder("d")
				.longOpt("database")
				.desc("The database to use.")
				.hasArg()
				.required()
				.build());
		
		options.addOption(
				Option.builder("t")
				.longOpt("tablename")
				.desc("The name of the table to create.")
				.hasArg()
				.required()
				.build());
			
		
	    /*
	     * Parse the commandline
	     */
	    
		CommandLineParser parser = new DefaultParser();
		CommandLine line;
		try {
			line = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Parsing failed. Reason: " + exp.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "SempalaNativeLoader", options );
			return;
		}
		
		// Get the arguments
	    hdfs_input = line.getOptionValue("input");
	    if (line.hasOption("prefixfile"))
	    	prefixfile = line.getOptionValue("prefixfile");
	    host = line.getOptionValue("host");
	    port = Integer.parseInt(line.getOptionValue("port", "21050"));
	    database = line.getOptionValue("database");
	    bigtable_tablename = line.getOptionValue("tablename");
			
	    
	    /*
	     * Load the data
	     */

// Why is this not necessary?
//		try {
//	    	Class.forName(JDBC_Driver);
//		} catch (ClassNotFoundException e) {
//			System.out.println("Where is your JDBC Driver?");
//			e.printStackTrace();
//			return;
//		}
//	    Enumeration<Driver> d = DriverManager.getDrivers();
//		while (d.hasMoreElements()) {
//			System.out.println(d.nextElement());
//		}
			
	    Connection connection = null;
		try {
			String impalad_url = String.format("jdbc:impala://%s:%d/%s", host, port, database);
			System.out.println(String.format("Connecting to %s", impalad_url));
			connection = DriverManager.getConnection(impalad_url);
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check host, port or database." );
			e.printStackTrace();
			return;
		}
	    
		try {
			Statement stmt = connection.createStatement();
			ResultSet resultSet;

		    final String tablename_external_raw = "external_raw";
		    final String tablename_internal_parquet = "internal_parquet";
		    final String tablename_distinct_subjects = "distinct_subjects";
		    
			// Import the table from hdfs
		    System.out.println("Import the table from hdfs @ " + hdfs_input);
			stmt.executeUpdate("CREATE EXTERNAL TABLE "+tablename_external_raw+" ( ID STRING, predicate STRING, object STRING )"
					+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY \"\\t\" LOCATION \"" + hdfs_input + "\";");

			// Create a new parquet table, partitioned by predicate");
			System.out.println("Creating parquet table");
			stmt.executeUpdate("CREATE TABLE "+tablename_internal_parquet+" ( ID STRING, object STRING ) PARTITIONED BY (predicate STRING) STORED AS PARQUET;");
			
			// Set parquet compression type 
			System.out.println("Setting parquet compression type");
			stmt.executeUpdate("set COMPRESSION_CODEC=snappy;");
			
			// Copy the contents from the inefficient to the efficient table
			System.out.println("Copying contents from external to parquet table");
			stmt.executeUpdate("INSERT OVERWRITE "+tablename_internal_parquet+" PARTITION (predicate) SELECT ID, object, predicate FROM "+tablename_external_raw+";");
			
			// Precompute optimization stats
			System.out.println("Precomputing optimization stats");
			stmt.executeUpdate("COMPUTE STATS "+tablename_internal_parquet+";");
			
			// Create distinct subjects
			System.out.println("Creating table containing distinct subjects");
			stmt.executeUpdate("CREATE TABLE "+tablename_distinct_subjects+" STORED AS PARQUET AS SELECT DISTINCT ID FROM "+tablename_internal_parquet+";");
			
			// Precompute optimization stats
			System.out.println("Precomputing optimization stats");
			stmt.executeUpdate("COMPUTE STATS "+tablename_distinct_subjects+";");
			
			// Prepare for building the bigtable
			StringBuilder select_clause_builder = new StringBuilder();
			StringBuilder from_clause_builder = new StringBuilder();
			select_clause_builder.append("t1.ID");
			from_clause_builder.append(tablename_distinct_subjects).append(" t1");
		    int table_counter = 2;
			
			// Get all properties
			resultSet = stmt.executeQuery("SELECT DISTINCT predicate FROM "+tablename_internal_parquet+";");
			while (resultSet.next()) {
				String predicate = resultSet.getString("predicate");
				
				// Append ", t<x>.object AS <predicate>" to select clause
				select_clause_builder.append(
						String.format(
								",\n t%1$d.object AS '%2$s'",
								table_counter,
								predicate)
						);

				// Append "LEFT JOIN <tablename_internal_parquet> t<x> ON (t1.ID = t<x>.ID AND t<x>.predicate = <predicate>)" to from clause
				from_clause_builder.append(
						String.format(
								" LEFT JOIN \n %1$s t%2$d ON (t1.ID = t%2$d.ID AND t%2$d.predicate = '%3$s')",
								tablename_internal_parquet,
								table_counter,
								predicate)
						);
				++table_counter;
			}

			// Create the bigtable 
			System.out.println("Creating bigtable");
			stmt.executeUpdate(
					String.format(
							"CREATE TABLE %1$s \n STORED AS PARQUET AS SELECT \n %2$s \n FROM \n %3$s",
							bigtable_tablename,
							select_clause_builder.toString(),
							from_clause_builder.toString())
					);
			
			// Precompute optimization stats
			System.out.println("Precomputing optimization stats");
			stmt.executeUpdate("COMPUTE STATS "+bigtable_tablename+";");
			
			// Drop the external table
			System.out.println("Dropping intermediate tables.");
			stmt.executeUpdate("DROP TABLE "+tablename_external_raw+";");
			stmt.executeUpdate("DROP TABLE "+tablename_internal_parquet+";");
			stmt.executeUpdate("DROP TABLE "+tablename_distinct_subjects+";");
			
			System.out.println("Loading data into a bigtable done.");
			connection.close();
		} catch (SQLException e) {
			System.out.println(e.getLocalizedMessage());
			e.printStackTrace();
		}
	}
}
