package de.uni_freiburg.informatik.dbis.sempala.loader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;

import de.uni_freiburg.informatik.dbis.sempala.loader.sql.CreateStatement;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.SelectStatement;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.CreateStatement.DataType;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.CreateStatement.FileFormat;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.InsertStatement;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.framework.Impala;

/**
 * 
 * @author Manuel Schneider <schneidm@informatik.uni-freiburg.de>
 *
 */
public final class SingleTableLoader extends Loader {
	
	/** The name of the intermediate table of distinct subject predicate relations */
	private final String tablename_distinct_sp_relations = "distinct_sp_relations";

	/** The name of the intermediate table of distinct object predicate relations */
	private final String tablename_distinct_op_relations = "distinct_op_relations";
	
	/** The constructor */
	public SingleTableLoader(Impala wrapper, String hdfsLocation){
		super(wrapper, hdfsLocation);
		tablename_output = "singletable";
	}

	/**
	 * Creates a new singletable from a triple table.
	 *
	 * The input table has to be in triple table format. The output will be a
	 * table in format described in 'Master's Thesis: S2RDF, Skilevic Simon'
	 *
	 * @throws SQLException
	 */
	@Override
	public void load() throws SQLException  {
		// Load the triple table
		buildTripleTable();

		/*
		 * Create a table for the distinct subject-predicate tuples (not partitioned)
		 */
		
		System.out.print(String.format("Creating table '%s'", tablename_distinct_sp_relations));
		long timestamp = System.currentTimeMillis();
		CreateStatement createStm = CreateStatement.createNew().tablename(tablename_distinct_sp_relations)
		.storedAs(FileFormat.PARQUET)
		.asSelect(
				SelectStatement.createNew().addProjection(column_name_subject)
				.addProjection(column_name_predicate)
				.distinct()
				.from(tablename_triple_table));
		frameworkWrapper.execute(createStm);
		
		System.out.println(String.format(" [%.3fs]", (float)(System.currentTimeMillis() - timestamp)/1000));
		frameworkWrapper.computeStats(tablename_distinct_sp_relations);

		/*
		 * Create a table for the distinct object-predicate tuples (not partitioned)
		 */

		System.out.print(String.format("Creating table '%s'", tablename_distinct_op_relations));
		timestamp = System.currentTimeMillis();
		createStm = CreateStatement.createNew()
		.tablename(tablename_distinct_op_relations)
		.storedAs(FileFormat.PARQUET)
		.ifNotExists()
		.asSelect(
				SelectStatement.createNew()
				.addProjection(column_name_object)
				.addProjection(column_name_predicate)
				.distinct()
				.from(tablename_triple_table));
		frameworkWrapper.execute(createStm);
		
		System.out.println(String.format(" [%.3fs]", (float)(System.currentTimeMillis() - timestamp)/1000));
		frameworkWrapper.computeStats(tablename_distinct_op_relations);

		/*
		 * Create the single table
		 */
		System.out.println(String.format("Starting the iterative creation of the singletable '%s'", tablename_output));
		timestamp = System.currentTimeMillis();
		
		// Get a list of all predicates
		ArrayList<String> predicates = new ArrayList<String>();
		SelectStatement selectStm = SelectStatement.createNew().addProjection(column_name_predicate).distinct().from(tablename_triple_table);
		ResultSet resultSet = frameworkWrapper.execute(selectStm);
		
		while (resultSet.next())
			predicates.add(resultSet.getString(column_name_predicate));
		
		// Create the new single table "s, p, o, [ss_p1, so_p1, os_p1], ..."
		CreateStatement cstmt = CreateStatement.createNew()
				.tablename(tablename_output)
				.addColumnDefinition(column_name_subject, DataType.STRING)
				.addPartitionDefinition(column_name_predicate, DataType.STRING)
				.addColumnDefinition(column_name_object, DataType.STRING);
		for (String pred : predicates){
			String impalaConformPred = toImpalaColumnName(pred);
			cstmt.addColumnDefinition(String.format("ss_%s", impalaConformPred), DataType.BOOLEAN);
			cstmt.addColumnDefinition(String.format("so_%s", impalaConformPred), DataType.BOOLEAN);
			cstmt.addColumnDefinition(String.format("os_%s", impalaConformPred), DataType.BOOLEAN);
		}
		cstmt.storedAs(FileFormat.PARQUET);
		frameworkWrapper.execute(cstmt);

		/*
		 * Fill the single table
		 */

		// Sets containing existing relations  
		HashSet<String> SS_relations = new HashSet<String>();
		HashSet<String> SO_relations = new HashSet<String>();
		HashSet<String> OS_relations = new HashSet<String>();
		
		for (String predicate : predicates){

			System.out.print(String.format("Processing '%s'", predicate));
			long localtimestamp = System.currentTimeMillis();
			
			// Reset existing relations 
			SS_relations.clear(); 
			SO_relations.clear(); 
			OS_relations.clear();
			
			// Get all predicates that are in a SS relation to any triples in this partition (predicate)
			selectStm = SelectStatement.createNew()
					.addProjection(column_name_predicate).distinct()
					.from(String.format("%s sp", tablename_distinct_sp_relations))
					.leftSemiJoin(
							String.format("%s tt", tablename_triple_table),
							String.format("tt.%s=sp.%s AND tt.%s='%s'",
									column_name_subject, column_name_subject, column_name_predicate, predicate),
							shuffle);
			resultSet = frameworkWrapper.execute(selectStm);
			while (resultSet.next())
				SS_relations.add(resultSet.getString(column_name_predicate));
			
    		// Get all predicates that are in a SO relation to any triples in this partition (predicate)
			selectStm = SelectStatement.createNew().addProjection(column_name_predicate).distinct()
					.from(String.format("%s op", tablename_distinct_op_relations))
					.leftSemiJoin(
							String.format("%s tt", tablename_triple_table),
							String.format("tt.%s=op.%s AND tt.%s='%s'",
									column_name_subject, column_name_object, column_name_predicate, predicate),
							shuffle);
			resultSet = frameworkWrapper.execute(selectStm);
			while (resultSet.next())
				SO_relations.add(resultSet.getString(column_name_predicate));
			
			// Get all predicates that are in a OS relation to any triples in this partition (predicate)
			selectStm = SelectStatement.createNew().addProjection(column_name_predicate).distinct()
					.from(String.format("%s sp", tablename_distinct_sp_relations))
					.leftSemiJoin(
							String.format("%s tt", tablename_triple_table),
							String.format("tt.%s=sp.%s AND tt.%s='%s'",
									column_name_object, column_name_subject, column_name_predicate, predicate),
							shuffle);
			resultSet = frameworkWrapper.execute(selectStm);
			
			while (resultSet.next())
				OS_relations.add(resultSet.getString(column_name_predicate));
			// Build a select stmt for the Insert-as-select statement
			SelectStatement ss = SelectStatement.createNew();

			// Build the huge select clause
			ss.addProjection(String.format("tt.%s", column_name_subject));
			ss.addProjection(String.format("tt.%s", column_name_object));
			ss.from(String.format("%s tt", tablename_triple_table));
			
			for (String p : predicates){
				String impalaConformPredicate = toImpalaColumnName(p);
				
				// SS_p_i
				if (SS_relations.contains(p)){
					ss.addProjection(String.format("CASE WHEN %s.%s IS NULL THEN false ELSE true END AS %s",
							String.format("tss_%s", impalaConformPredicate),
							column_name_subject,
							String.format("ss_%s", impalaConformPredicate)));
					ss.leftJoin(
							// Table reference e.g. "LEFT JOIN subjects tss_p1"
							String.format("%s tss_%s", tablename_distinct_sp_relations, impalaConformPredicate),
							// On clause e.g. "ON tt.id=tss_p1.id AND tss_p1.predicate='wsdbm:friendOf'"
							String.format("tt.%1$s=tss_%3$s.%1$s AND tss_%3$s.%2$s='%4$s'",
									column_name_subject,
									column_name_predicate,
									impalaConformPredicate,
									p),
		   	    			shuffle);
				} else {
					ss.addProjection("false");
				}

				// SO_pi
				if (SO_relations.contains(p)){
					ss.addProjection(String.format("CASE WHEN %s.%s IS NULL THEN false ELSE true END AS %s",
							String.format("tso_%s", impalaConformPredicate),
							column_name_object,
							String.format("so_%s", impalaConformPredicate)));
					ss.leftJoin(
							// Table reference e.g. "LEFT JOIN objects tso_p1"
							String.format("%s tso_%s", tablename_distinct_op_relations, impalaConformPredicate),
							// On clause e.g. "ON tt.id=tso_p1.object AND tso_p1.predicate='wsdbm:friendOf'"
							String.format("tt.%1$s=tso_%4$s.%3$s AND tso_%4$s.%2$s='%5$s'",
									column_name_subject,
									column_name_predicate,
									column_name_object,
									impalaConformPredicate,
									p),
		   	    			shuffle);
				} else {
					ss.addProjection("false");
				}
				
				if (OS_relations.contains(p)){
					ss.addProjection(String.format("CASE WHEN %s.%s IS NULL THEN false ELSE true END AS %s",
							String.format("tos_%s", impalaConformPredicate),
							column_name_subject,
							String.format("os_%s", impalaConformPredicate)));
					ss.leftJoin(
							// Table reference e.g. "LEFT JOIN subjects tos_p1"
							String.format("%s tos_%s", tablename_distinct_sp_relations, impalaConformPredicate),
							// On clause e.g. "ON tt.object=tos_p1.id AND tos_p1.predicate='wsdbm:friendOf'"
							String.format("tt.%3$s=tos_%4$s.%1$s AND tos_%4$s.%2$s='%5$s'",
									column_name_subject,
									column_name_predicate,
									column_name_object,
									impalaConformPredicate,
									p),
		   	    			shuffle);
					
				} else {
					ss.addProjection("false");
				}
			}
			// Partition column at last (impala requirement)
			ss.addProjection(String.format("tt.%s", column_name_predicate));
			ss.where(String.format("tt.%s='%s'", column_name_predicate, predicate));
			
			// Insert data into the single table using the built select stmt
			InsertStatement insertStatement = InsertStatement.createNew().overwrite().tablename(tablename_output)
			.addPartition(column_name_predicate)
			.selectStatement(ss);	
			frameworkWrapper.execute(insertStatement);
			
			System.out.println(String.format(" [%.3fs]", (float)(System.currentTimeMillis() - localtimestamp)/1000));
		}
		System.out.println(String.format("Singletable created in [%.3fs]", (float)(System.currentTimeMillis() - timestamp)/1000));
		frameworkWrapper.computeStats(tablename_output);
		
		// Drop intermediate tables
		if (!keep){
			frameworkWrapper.dropTable(tablename_triple_table);
			frameworkWrapper.dropTable(tablename_distinct_sp_relations);
			frameworkWrapper.dropTable(tablename_distinct_op_relations);
		}
	}
}