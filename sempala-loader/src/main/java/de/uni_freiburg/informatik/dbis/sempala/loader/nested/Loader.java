import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

public class Loader {
	
	public static void buildTripleTable(String path, HiveContext hc){
		hc.sql("CREATE EXTERNAL TABLE triple_table(s STRING, p STRING, o STRING) "
				 + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' "
				+ "LOCATION '" + path + "'");
	}
	
	private static String getValidName(String name){
		return name.replaceAll("[^a-zA-Z0-9_]", "_");
	}
	
	public static ArrayList<String> getProperties(String tableName, HiveContext hc){
		Row[] props = hc.sql("SELECT DISTINCT p FROM " + tableName).collect();
		ArrayList<String> properties = new ArrayList<String>();
		for (int i = 0; i < props.length; i++) {
			properties.add(props[i].getString(0));
		}
		return properties;
	}
	

	public static void savePropertiesIntoTable(HiveContext hc, String tableName) {
		// return rows of format <predicate, is_complex>
		// is_complex can be 1 or 0
		// 1 for multivalued predicate, 0 for single predicate

		// select the properties that are complex
		DataFrame multivaluedProperties = hc.sql("SELECT DISTINCT(p) AS p FROM "
				+ "(SELECT s, p, COUNT(*) AS rc FROM triple_table GROUP BY s, p HAVING rc > 1) AS grouped");
		
		// select all the properties
		DataFrame allProperties = hc.sql("SELECT DISTINCT(p) AS p FROM triple_table");

		// select the properties that are not complex
		DataFrame singledValueProperties = allProperties.except(multivaluedProperties);
		
		// combine them
		DataFrame combinedProperties = singledValueProperties.selectExpr("p", "0 AS is_complex").
				unionAll(multivaluedProperties.selectExpr("p", "1 as is_complex"));
		
		// write the result
		combinedProperties.write().mode(SaveMode.Overwrite).saveAsTable(tableName);
	}
	
	
	/*
	 * Create the final property table, allProperties contains the list of all possible properties
	 * isComplexProperty contains (in the same order used by allProperties) the boolean value that indicates
	 * if that property is complex (called also multi valued) or simple.
	 */
	public static void buildPropertyTable(HiveContext hc, String[] allProperties, Boolean[] isComplexProperty){
		
		// create a new aggregation environment
		PropertiesAggregateFunction2 aggregator = new PropertiesAggregateFunction2(allProperties);
		
		// get the compressed table
		DataFrame compressedTriples = hc.sql("SELECT s, CONCAT(p, ' ', o) AS po FROM triple_table");
		// group by the subject and get all the data
		DataFrame grouped = compressedTriples.groupBy("s")
				.agg(aggregator.apply(compressedTriples.col("po")).alias("group"));
		
		// build the query to extract the property from the array
		String[] selectProperties = new String[allProperties.length + 1];
		selectProperties[0] = "s";
		for(int i = 0; i < allProperties.length; i++){
			// if is not a complex type, extract the value
			String newProperty = isComplexProperty[i] ? " " + "group[" + String.valueOf(i) + "] as " + getValidName(allProperties[i])
						: " " + "group[" + String.valueOf(i) + "][0] as " + getValidName(allProperties[i]);
			selectProperties[i+1] = newProperty;
		}
		
		DataFrame propertyTable = grouped.selectExpr(selectProperties);
		
		// write the final one
		propertyTable.write().mode(SaveMode.Overwrite).format("parquet").saveAsTable("property_table");
	}
	
	// drop all the unnecessary tables
	public static void dropTemporaryTables(HiveContext hc, String... tableNames){
		for(String tb : tableNames)
			hc.sql("DROP TABLE " + tb);
	}
	

}
