package de.uni_freiburg.informatik.dbis.sempala.impala;

import java.sql.Connection;
import java.sql.SQLException;

public final class InsertStatement {
	
	private Connection connection; 
	
	private String tablename = null;
	private Boolean overwrite = false;
	private String partitions = null;
	private String selectStatement = null;

	public InsertStatement(Connection connection, String tablename) {
		this.connection = connection;
		this.tablename = tablename;
	}

	public int execute() throws IllegalArgumentException, SQLException {
		return connection.createStatement().executeUpdate(toString());
	}

	public InsertStatement tablename(String tablename) {
		this.tablename = tablename;
		return this;
	}

	public InsertStatement overwrite() {
		this.overwrite = true;
		return this;
	}

	public InsertStatement addPartition(final String partition) {
		if (this.partitions==null)
			partitions = partition;
		else
			partitions+=String.format(", %s", partition);
		return this;
	}
	
	public InsertStatement selectStatement(final String selectStatement) {
		this.selectStatement = selectStatement;
		return this;
	}
	
	public InsertStatement selectStatement(final SelectStatement selectStatement) {
		this.selectStatement = selectStatement.toString();
		return this;
	}

	public String toString() throws IllegalArgumentException {
		if (this.tablename == null || this.selectStatement == null)
			throw new IllegalArgumentException("tablename and either some columnDefinitions or asSelect must be specified");
		/*
		 * [with_clause]
	     * INSERT { INTO | OVERWRITE } [TABLE] table_name
	     *   [(column_list)]
	     *   [ PARTITION (partition_clause)]
	     * {
	     *     [hint_clause] select_statement
	     *   | VALUES (value [, value ...]) [, (value [, value ...]) ...]
	     * }
	     * 
	     * partition_clause ::= col_name [= constant] [, col_name [= constant] ...]
	     * 
	     * hint_clause ::= [SHUFFLE] | [NOSHUFFLE]    (Note: the square brackets are part of the syntax.)
		 */
		
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("INSERT%s TABLE %s", this.overwrite ? " OVERWRITE" : " INTO", this.tablename));
		if (partitions != null)
			sb.append(String.format("\nPARTITION (%s)", partitions));
		if (this.selectStatement != null)
			sb.append(String.format("\n%s", this.selectStatement));
		return sb.toString();
	}

}
