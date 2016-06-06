package de.uni_freiburg.informatik.dbis.sempala.loader.sql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * This is an Java binding for Impala JDBC.
 *
 * This class lets you write Impala queries in Java. The statesments that are
 * sent to the impala daemon are build with convenient builders and finally
 * executed. This class is work in progress and is nowhere near final. Things
 * are getting implemented not until they are needed in the context of sempala.
 * If you miss something implement it!
 *
 * @author Manuel Schneider <schneidm@informatik.uni-freiburg.de>
 *
 */
public final class Impala {

	/** An enumertation of the query options supported by impala 2.2 */
	public enum QueryOption {
		ABORT_ON_DEFAULT_LIMIT_EXCEEDED,
		ABORT_ON_ERROR,
		ALLOW_UNSUPPORTED_FORMATS,
		APPX_COUNT_DISTINCT,
		BATCH_SIZE,
		COMPRESSION_CODEC,
		DEBUG_ACTION,
		DEFAULT_ORDER_BY_LIMIT,
		DISABLE_CODEGEN,
		DISABLE_UNSAFE_SPILLS,
		EXEC_SINGLE_NODE_ROWS_THRESHOLD,
		EXPLAIN_LEVEL,
		HBASE_CACHE_BLOCKS,
		HBASE_CACHING,
		MAX_ERRORS,
		MAX_IO_BUFFERS,
		MAX_SCAN_RANGE_LENGTH,
		MEM_LIMIT,
		NUM_NODES,
		NUM_SCANNER_THREADS,
		PARQUET_COMPRESSION_CODEC,
		PARQUET_FILE_SIZE,
		QUERY_TIMEOUT_S,
		REQUEST_POOL,
		RESERVATION_REQUEST_TIMEOUT,
		SUPPORT_START_OVER,
		SYNC_DDL,
		V_CPU_CORES
	}

	/** The connection to the impala daemon */
    private Connection connection = null;

    /** Creates an instance of the impala wrapper. */
	public Impala(String host, String port, String database) throws SQLException {
        // Dynamically load the impala driver // Why is this not necessary?
//		try {
//	    	Class.forName("com.cloudera.impala.jdbc41.Driver");
//		} catch (ClassNotFoundException e) {
//			System.out.println("Where is your JDBC Driver?");
//			e.printStackTrace();
//			return;
//		}
//	    Enumeration<Driver> d = DriverManager.getDrivers();
//		while (d.hasMoreElements()) {
//			System.out.println(d.nextElement());
//		}

    	// Establish the connection to impalad
		String impalad_url = String.format("jdbc:impala://%s:%s/", host, port);
		System.out.println(String.format("Connecting to impalad (%s)", impalad_url));
		connection = DriverManager.getConnection(impalad_url);
		try {
			// Try to create the database
			connection.createStatement().executeUpdate(String.format("CREATE DATABASE %s", database));
		} catch (SQLException e) {
			// TODO: Make sure this is the exception we expect (database exists)
			// Ask which action should be taken
			inputloop: while (true) {
				System.out.printf("Database '%s' exists. (D)rop it, (u)se it, (a)bort? [d/u/a]: ", database);
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				String input = null;
				try {
					input = br.readLine().toLowerCase();
				} catch (IOException e1) {
					e1.printStackTrace();
					System.exit(1);
				}
				switch (input) {
				case "d":
					connection.createStatement().executeUpdate(String.format("DROP DATABASE %s CASCADE", database));
					connection.createStatement().executeUpdate(String.format("CREATE DATABASE IF NOT EXISTS %s", database));
					break inputloop;
				case "u":
					break inputloop;
				case "a":
					System.exit(1);
				}
			}
		}
		connection.createStatement().executeUpdate(String.format("USE %s", database));
    }

    @Override
    protected void finalize() throws Throwable {
    	connection.close();
    	connection=null;
    	super.finalize();
    }

    /**
     * Creates a handy builder for the CREATE statement.
     *
     * @param tablename The name of the table you want to create.
     * @return The builder for the CREATE statement.
     */
    public CreateStatement createTable(String tablename){
    	return new CreateStatement(connection, tablename);
    }

    /**
     * Creates a handy builder for the CREATE statement.
     *
     * Convenience function for {@link createTable}. This function returns a
     * statement with the external flag initially set.
     *
     * @param tablename The name of the table you want to create.
     * @return The builder for the CREATE statement.
     */
    public CreateStatement createTable(){
    	return new CreateStatement(connection);
    }

    /**
     * Creates a handy builder for the INSERT statement.
     *
     * @param tablename The name of the table you want to insert into.
     * @return The builder for the INSERT statement.
     */
    public InsertStatement insertInto(String tablename){
    	return new InsertStatement(connection, tablename);
    }

    /**
     * Creates a handy builder for the INSERT statement.
     *
     * Convenience function for {@link insertTable}. This function returns a
     * statement with the overwrite flag initially set.
     *
     * @param tablename The name of the table you want to create.
     * @return The builder for the INSERT statement.
     */
    public InsertStatement insertOverwrite(String tablename){
    	return this.insertInto(tablename).overwrite();
    }

    /**
     * Creates a handy builder for the SELECT statement.
     *
     * @param tablename The projection expression of your query
     * @return The builder for the SELECT statement.
     */
    public SelectStatement select(){
    	return new SelectStatement(connection);
    }

    /**
     * Creates a handy builder for the SELECT statement.
     *
     * @param tablename The projection expression of your query
     * @return The builder for the SELECT statement.
     */
    public SelectStatement select(String expression){
    	return new SelectStatement(connection, expression);
    }

    /**
     * Drops a table instantly.
     *
     * @param tablename The table to drop.
     * @throws SQLException
     */
    public void dropTable(String tablename) throws SQLException {
		System.out.print(String.format("Dropping table '%s'", tablename));
		long startTime = System.currentTimeMillis();
		connection.createStatement().executeUpdate(String.format("DROP TABLE %s;", tablename));
		long endTime = System.currentTimeMillis();
		System.out.println(String.format(" [%.3fs]", (float)(endTime - startTime)/1000));
    }

    /**
     * Computes stats for a table (optimization)
     *
     * Gathers information about volume and distribution of data in a table and
     * all associated columns and partitions. The information is stored in the
     * metastore database, and used by Impala to help optimize queries.
     *
     * http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/impala_perf_stats.html?scroll=perf_stats
     *
     * @param tablename
     * @throws SQLException
     */
    public void computeStats(String tablename) throws SQLException {
		System.out.print(String.format("Precomputing optimization stats for '%s'", tablename));
		long startTime = System.currentTimeMillis();
		connection.createStatement().executeUpdate(String.format("COMPUTE STATS %s;", tablename));
		long endTime = System.currentTimeMillis();
		System.out.println(String.format(" [%.3fs]", (float)(endTime - startTime)/1000));
    }

    /**
     * Sets an impala query option
     *
     * http://www.cloudera.com/content/www/en-us/documentation/archive/impala/2-x/2-1-x/topics/impala_query_options.html?scroll=query_options
     *
     * @param option The option to set.
     * @param value The value of the option to set.
     * @throws SQLException
     */
    public void set(QueryOption option, String value) throws SQLException {
		System.out.print(String.format("Setting impala query option '%s' to '%s'", option.name(), value));
		long startTime = System.currentTimeMillis();
		connection.createStatement().executeUpdate(String.format("SET %s=%s;", option.name(), value));
		long endTime = System.currentTimeMillis();
		System.out.println(String.format(" [%.3fs]", (float)(endTime - startTime)/1000));
    }
}
