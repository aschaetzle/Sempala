package de.uni_freiburg.informatik.dbis.sempala.loader.sql.framework;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import de.uni_freiburg.informatik.dbis.sempala.loader.sql.CreateStatement;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.InsertStatement;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.SelectStatement;

/**
 * Wrapper of Spark connection. This class contains the initialization of needed
 * Spark objects. It wraps the functionality for execution of Spark SQL queries.
 * 
 * @author Polina Koleva
 *
 */
public class Spark implements Framework {

	private SparkConf sparkConfiguration;
	private JavaSparkContext javaContext;
	private HiveContext hiveContext;

	/**
	 * Initializes a Spark connection. Use it afterwards for execution of Spark
	 * SQL queries.
	 * 
	 * @param appName
	 *            the name of the app that will be used with this Spark
	 *            connection
	 * @param database
	 *            name of the database that will be used with this Spark
	 *            connection
	 */
	public Spark(String appName, String database) {

		// TODO check what will happen if there is already in use the same app
		// name
		this.sparkConfiguration = new SparkConf().setAppName(appName);
		this.javaContext = new JavaSparkContext(sparkConfiguration);
		this.hiveContext = new HiveContext(javaContext);
		// TODO check what kind of exception can be thrown here if there is a
		// problem with spark connection

		// try {
		// Try to create the database
		this.getHiveContext().sql(String.format("CREATE DATABASE %s", database));
		// TODO check what kind of exception is thrown if database already
		// exists
		// } catch (SQLException e) {
		// inputloop: while (true) {
		// System.out.printf("Database '%s' exists. (D)rop it, (u)se it,
		// (a)bort? [d/u/a]: ", database);
		// BufferedReader br = new BufferedReader(new
		// InputStreamReader(System.in));
		// String input = null;
		// try {
		// input = br.readLine().toLowerCase();
		// } catch (IOException e1) {
		// e1.printStackTrace();
		// System.exit(1);
		// }
		// switch (input) {
		// case "d":
		// hiveContext.sql(String.format("DROP DATABASE %s CASCADE", database));
		// hiveContext.sql((String.format("CREATE DATABASE IF NOT EXISTS %s",
		// database)));
		// break inputloop;
		// case "u":
		// break inputloop;
		// case "a":
		// System.exit(1);
		// }
		// }
		// }
		this.hiveContext.sql((String.format("USE %s", database)));
	}

	/**
	 * Get Hive context.
	 * 
	 * @return {@link HiveContext}
	 */
	public HiveContext getHiveContext() {
		return hiveContext;
	}

	@Override
	public void execute(CreateStatement createStm) {
		// TODO catch exception if possible
		this.hiveContext.sql(createStm.toString());
	}

	@Override
	public void execute(InsertStatement insertStm) {
		// TODO catch exception if possible
		this.hiveContext.sql(insertStm.toString());
	}

	@Override
	public void computeStats(String tablename) {
		// TODO implement if needed
	}

	@Override
	public void dropTable(String tablename) {
		// TODO catch exception if possible
		this.hiveContext.sql("DROP TABLE " + tablename);
	}

	@Override
	public DataFrame execute(SelectStatement selectStm) {
		// TODO catch exception if possible
		return this.hiveContext.sql(selectStm.toString());

	}

}
