package de.uni_freiburg.informatik.dbis.sempala.translator.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Wrapper of Spark connection. This class contains the initialization of needed
 * Spark objects. It wraps the functionality for execution of Spark SQL queries.
 * 
 * @author Polina Koleva
 *
 */
public class Spark {

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
	 * @param master
	 *            the master URI
	 */
	public Spark(String appName, String database) {

		// TODO check what will happen if there is already in use the same app
		// name
		this.sparkConfiguration = new SparkConf().setAppName(appName);
		this.javaContext = new JavaSparkContext(sparkConfiguration);
		this.hiveContext = new HiveContext(javaContext);
	}

	/**
	 * Get Hive context.
	 * 
	 * @return {@link HiveContext}
	 */
	public HiveContext getHiveContext() {
		return hiveContext;
	}

	/**
	 * Execute a query and return its result.
	 */
	public DataFrame sql(String sqlQuqery) {
		return this.hiveContext.sql(sqlQuqery);
	}
}
