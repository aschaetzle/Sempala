package de.uni_freiburg.informatik.dbis.sempala.impala;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class SelectStatement {
	
	private Connection connection; 

	private boolean distinct = false;
	private String projections = null;
	private String from = null;
	private String where = null;
	private String groupby = null;
	private String having = null;
	private String limit = null;

	public SelectStatement(Connection connection, String projections) {
		this.connection = connection;
		this.projections = projections;
	}

	public ResultSet execute() throws IllegalArgumentException, SQLException {
		return connection.createStatement().executeQuery(toString());
	}

	public SelectStatement distinct() {
		this.distinct = true;
		return this;
	}
	
	public SelectStatement addProjection(final String projection) {
		if (projections==null)
			projections = projection;
		else
			projections+=String.format(", %s", projection);
		return this;
	}

	public SelectStatement from(final String from) {
		this.from = from;
		return this;
	}

	public SelectStatement where(final String where) {
		this.where = where;
		return this;
	}

	public SelectStatement groupby(final String groupby) {
		this.groupby = groupby;
		return this;
	}

	public SelectStatement having(final String having) {
		this.having = having;
		return this;
	}

	public SelectStatement limit(final String limit) {
		this.limit = limit;
		return this;
	}

	public String toString() throws IllegalArgumentException {
		if (this.projections.isEmpty() || this.from == null)
			throw new IllegalArgumentException("Projections and source must be specified");

		StringBuilder sb = new StringBuilder();
		sb.append(String.format("SELECT%s %s", this.distinct ? " DISTINCT": "", projections));
		sb.append(String.format("\nFROM %s", this.from));
		if (this.where != null)
			sb.append(String.format("\nWHERE %s", this.where));
		if (this.groupby != null)
			sb.append(String.format("\nGROUP BY %s", this.groupby));
		if (this.having != null)
			sb.append(String.format("\nHAVING %s", this.having));
		if (this.limit != null)
			sb.append(String.format("\nLIMIT %s", this.limit));
		return sb.toString();
	}
}
