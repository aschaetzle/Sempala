package de.uni_freiburg.informatik.dbis.sempala.loader;

import java.sql.SQLException;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import de.uni_freiburg.informatik.dbis.sempala.loader.sql.SelectStatement;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.framework.Spark;
import de.uni_freiburg.informatik.dbis.sempala.loader.udf.PropertiesAggregateFunction;

// TODO add class comments
public class ComplexPropertyTableLoader extends Loader {

	protected static final String tablename_properties = "properties";
	protected static final String tablename_complex_property_table = "complex_property_table";
	protected static final String column_name_is_complex = "is_complex";

	protected static final String table_format_parquet = "parquet";

	public ComplexPropertyTableLoader(Spark wrapper, String hdfsLocation) {
		super(wrapper, hdfsLocation);
	}

	// TODO add comments
	private static String getValidName(String name) {
		return name.replaceAll("[^a-zA-Z0-9_]", "_");
	}

	// TODO add comments
	public void savePropertiesIntoTable(HiveContext hiveContext, String tableName) {
		// return rows of format <predicate, is_complex>
		// is_complex can be 1 or 0
		// 1 for multivalued predicate, 0 for single predicate

		// TODO try to override these queries with select statement objects
		// SelectStatement multiPropertiesSelection =
		// SelectStatement.createNew().addProjection(column_name_predicate).distinct().from(from)

		// select the properties that are complex		
		DataFrame multivaluedProperties = hiveContext.sql(String.format(
				"SELECT DISTINCT(%1$s) AS %1$s FROM (SELECT %2$s, %1$s, COUNT(*) AS rc FROM %3$s GROUP BY %2$s, %1$s HAVING rc > 1) AS grouped",
				column_name_predicate, column_name_subject, tablename_triple_table));

		// TODO try to override these queries with select statement objects
		// SelectStatement allPropertiesSelection =
		// SelectStatement.createNew().addProjection(column_name_predicate)
		// .distinct().from(tablename_triple_table);
		// DataFrame allProperties =
		// frameworkWrapper.execute(allPropertiesSelection);

		// select all the properties
		DataFrame allProperties = hiveContext.sql(String.format("SELECT DISTINCT(%1$s) AS %1$s FROM %2$s",
				column_name_predicate, tablename_triple_table));

		// select the properties that are not complex
		DataFrame singledValueProperties = allProperties.except(multivaluedProperties);

		// combine them
		DataFrame combinedProperties = singledValueProperties
				.selectExpr(column_name_predicate, "0 AS " + column_name_is_complex)
				.unionAll(multivaluedProperties.selectExpr(column_name_predicate, "1 AS " + column_name_is_complex));

		// write the result
		combinedProperties.write().mode(SaveMode.Overwrite).saveAsTable(tableName);
	}

	/*
	 * Create the final property table, allProperties contains the list of all
	 * possible properties isComplexProperty contains (in the same order used by
	 * allProperties) the boolean value that indicates if that property is
	 * complex (called also multi valued) or simple.
	 */
	public void buildPropertyTable(HiveContext hc, String[] allProperties, Boolean[] isComplexProperty) {

		// create a new aggregation environment
		PropertiesAggregateFunction aggregator = new PropertiesAggregateFunction(allProperties);

		String predicateObjectColumn = "po";
		String groupColumn = "group";
		
		// get the compressed table
		DataFrame compressedTriples = hc.sql(String.format("SELECT %s, CONCAT(%s, ' ', %s) AS po FROM %s", column_name_subject, column_name_predicate, column_name_object, tablename_triple_table));
		
		// group by the subject and get all the data
		DataFrame grouped = compressedTriples.groupBy(column_name_subject)
				.agg(aggregator.apply(compressedTriples.col(predicateObjectColumn)).alias(groupColumn));

		// build the query to extract the property from the array
		String[] selectProperties = new String[allProperties.length + 1];
		selectProperties[0] = "s";
		for (int i = 0; i < allProperties.length; i++) {
			// if is not a complex type, extract the value
			String newProperty = isComplexProperty[i]
					? " " + groupColumn + "[" + String.valueOf(i) + "] AS " + getValidName(allProperties[i])
					: " " + groupColumn + "[" + String.valueOf(i) + "][0] AS " + getValidName(allProperties[i]);
			selectProperties[i + 1] = newProperty;
		}

		DataFrame propertyTable = grouped.selectExpr(selectProperties);

		// write the final one
		propertyTable.write().mode(SaveMode.Overwrite).format(table_format_parquet)
				.saveAsTable(tablename_complex_property_table);
	}

	@Override
	public void load() throws SQLException {
		buildTripleTable();

		Spark spark = (Spark) frameworkWrapper;
		// table name - properties
		savePropertiesIntoTable(spark.getHiveContext(), tablename_properties);

		
		// TODO override with select stamement
		//SelectStatement selectStm = SelectStatement.createNew().from(tablename_properties);
		//Row[] props = spark.execute(selectStm).collect();
		
		Row[] props =  spark.getHiveContext().sql(String.format("SELECT * FROM %s", tablename_properties)).collect();
		String[] allProperties = new String[props.length];
		Boolean[] isComplexProperty = new Boolean[props.length];
		for (int i = 0; i < props.length; i++) {
			allProperties[i] = props[i].getString(0);
			isComplexProperty[i] = props[i].getInt(1) == 1;
		}

		buildPropertyTable(spark.getHiveContext(), allProperties, isComplexProperty);

		// Drop intermediate tables
		if (!keep) {
			frameworkWrapper.dropTable(tablename_triple_table);
		}
	}

}
