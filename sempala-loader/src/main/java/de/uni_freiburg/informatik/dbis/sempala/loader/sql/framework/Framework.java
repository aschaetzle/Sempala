package de.uni_freiburg.informatik.dbis.sempala.loader.sql.framework;

import de.uni_freiburg.informatik.dbis.sempala.loader.sql.CreateStatement;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.InsertStatement;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.SelectStatement;

/**
 * Interface that is implemented by different underlying platforms used in the
 * project. For example, for the construction and loading of complex property
 * table Spark is used. Therefore, the class which initializes a connection to
 * Spark implements this interface. It contains methods for executing of
 * queries, dropping tables and computing statistics.
 * 
 * @author Polina Koleva
 *
 */
public interface Framework {

	/**
	 * Executes a create SQL query.
	 * 
	 * @param createStm
	 *            SQL create table query
	 */
	public void execute(CreateStatement createStm);

	/**
	 * Executes an insert SQL query.
	 * 
	 * @param insertStm
	 *            SQL insert query
	 */
	public void execute(InsertStatement insertStm);

	/**
	 * Executes a select SQL statement and returns the selected rows.
	 * 
	 * @param selectStm
	 *            SQL select query
	 * @return the result returned from the select query
	 */
	public <T> T execute(SelectStatement selectStm);

	/**
	 * Computes statistics for a table.
	 * 
	 * @param tablename
	 *            name of the table for which statistics is computed
	 */
	public void computeStats(String tablename);

	/**
	 * Drops a table.
	 * 
	 * @param tablename
	 *            name of the table that is dropped
	 */
	public void dropTable(String tablename);

}
