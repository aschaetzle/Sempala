package de.uni_freiburg.informatik.dbis.sempala.loader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import de.uni_freiburg.informatik.dbis.sempala.loader.sql.CreateStatement;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.Impala;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.SelectStatement;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.CreateStatement.DataType;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.CreateStatement.FileFormat;

/**
 * 
 * @author Manuel Schneider <schneidm@informatik.uni-freiburg.de>
 *
 */
public final class PropertyTableLoader extends Loader {
	
	/** The name of the intermediate table of distinct subjects */
	private final String tablename_distinct_subjects = "distinct_subjects";

	/** The constructor */
	public PropertyTableLoader(Impala wrapper, String hdfsLocation){
		super(wrapper, hdfsLocation);	
		tablename_output = "propertytable";	
	}

	/**
	 * Creates a new property table from a triple table.
	 *
	 * The input table has to be in triple table format. The output will be a
	 * table in format described in 'Sempala: Interactive SPARQL Query
	 * Processing on Hadoop'.
	 *
	 * @throws SQLException
	 */
	@Override
	public void load() throws SQLException {
		// Load the triple table
		buildTripleTable();
		
		// Build a table containing distinct subjects
		System.out.print(String.format("Creating table containing distinct subjects (%s)", tablename_distinct_subjects));
		long timestamp = System.currentTimeMillis();
		impala
		.createTable(tablename_distinct_subjects)
		.storedAs(FileFormat.PARQUET)
		.asSelect(
				impala
				.select(column_name_subject)
				.distinct()
				.from(tablename_triple_table)
				)
		.execute();
		System.out.println(String.format(" [%.3fs]", (float)(System.currentTimeMillis() - timestamp)/1000));
		impala.computeStats(tablename_distinct_subjects);

		System.out.print(String.format("Creating property table (%s)", tablename_output));
		timestamp = System.currentTimeMillis();
		
		// Get properties
		ResultSet resultSet = impala
				.select(column_name_predicate)
				.distinct()
				.from(tablename_triple_table)
				.execute();

		// Convert the result set to a list
		ArrayList<String> predicates = new ArrayList<String>();
		while (resultSet.next())
			predicates.add(resultSet.getString(column_name_predicate));

		// Build a select stmt for the Insert-as-select statement
		SelectStatement sstmt = impala.select();

		// Add the subject column
		sstmt.addProjection(String.format("subjects.%s", column_name_subject));

		// Add the property columns to select clause (", t<x>.object AS <predicate>")
   	    for (int i = 0; i < predicates.size(); i++)
			sstmt.addProjection(String.format("t%d.%s AS %s", i, column_name_object, toImpalaColumnName(predicates.get(i))));

		// Add distinct subjects table reference
		sstmt.from(String.format("%s subjects", tablename_distinct_subjects));

		// Append the properties via join
		// "LEFT JOIN <tablename_internal_parquet> t<x> ON (t1.subject =
		// t<x>.subject AND t<x>.predicate = <predicate>)" to from clause
		for (int i = 0; i < predicates.size(); i++)
   	    	sstmt.leftJoin(
   	    			String.format("%s t%d", tablename_triple_table, i),
   	    			String.format("subjects.%2$s = t%1$d.%2$s AND t%1$d.%3$s = '%4$s'",
   	    					i, column_name_subject,
   	    					column_name_predicate, predicates.get(i)),
   	    			shuffle);

		// Create the property table "s, p, o[, p1, ...]"
		CreateStatement cstmt = impala.createTable(tablename_output).ifNotExists();
		cstmt.addColumnDefinition(column_name_subject, DataType.STRING);
		for (String pred : predicates)
			cstmt.addColumnDefinition(toImpalaColumnName(pred), DataType.STRING);
		cstmt.storedAs(FileFormat.PARQUET);
		cstmt.execute();
		
		// Insert data into the single table using the built select stmt
		impala
		.insertOverwrite(tablename_output)
		.selectStatement(sstmt)
		.execute();
		System.out.println(String.format(" [%.3fs]", (float)(System.currentTimeMillis() - timestamp)/1000));
		impala.computeStats(tablename_output);
		
		// Drop intermediate tables
		if (!keep){
			impala.dropTable(tablename_triple_table);
			impala.dropTable(tablename_distinct_subjects);
		}
	}
}
