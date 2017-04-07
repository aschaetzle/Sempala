package de.uni_freiburg.informatik.dbis.sempala.translator.spark;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import de.uni_freiburg.informatik.dbis.sempala.translator.Tags;

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
	private int dfPartitions;
	
	/**
	 * Initializes a Spark connection. Use it afterwards for execution of Spark
	 * SQL queries.
	 * 
	 * @param appName the name of the app that will be used with this Spark
	 *            connection
	 * @param database name of the database that will be used with this Spark
	 *            connection
	 * @param master the master URI
	 */
	public Spark(String appName, String database) {

		// TODO check what will happen if there is already in use the same app
		// name
		this.sparkConfiguration = new SparkConf().setAppName(appName).set("spark.io.compression.codec", "snappy");
		this.javaContext = new JavaSparkContext(sparkConfiguration);
		this.hiveContext = new HiveContext(javaContext);

		// use the created database
		this.hiveContext.sql((String.format("USE %s", database)));

		configureSparkContext();
		cacheTable();

	}

	/**
	 * Add specific configuration for Spark SQL execution.
	 */
	public void configureSparkContext(){
		Properties properties = new Properties();
		properties.setProperty("spark.sql.parquet.filterPushdown", "true");
		properties.setProperty("spark.sql.inMemoryColumnarStorage.batchSize", "20000");
		if(dfPartitions != 0){
			properties.setProperty("spark.sql.shuffle.partitions", Integer.toString(dfPartitions));
		}
		
		hiveContext.setConf(properties);
	}
	
	/**
	 * Cache complex_property_table.
	 */
	public void cacheTable() {
		DataFrame result = hiveContext.sql(String.format("SELECT * FROM %s",
				Tags.COMPLEX_PROPERTYTABLE_TABLENAME));
		// add partitioning if enables
		if (dfPartitions != 0) {
			result.repartition(dfPartitions);
		}
		
		result.registerTempTable(Tags.CACHED_COMPLEX_PROPERTYTABLE_TABLENAME);
		hiveContext.cacheTable(Tags.CACHED_COMPLEX_PROPERTYTABLE_TABLENAME);
		
		// force caching
		result.count();
	}

	public void unpersistCachedTables(){
		hiveContext.uncacheTable(Tags.CACHED_COMPLEX_PROPERTYTABLE_TABLENAME);
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

	public int getDfPartitions() {
		return dfPartitions;
	}

	public void setDfPartitions(int dfPartitions) {
		this.dfPartitions = dfPartitions;
	}
}
