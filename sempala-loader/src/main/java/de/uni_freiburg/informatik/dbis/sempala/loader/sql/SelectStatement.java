package de.uni_freiburg.informatik.dbis.sempala.loader.sql;

import java.util.ArrayList;

//TODO add comments if time left
/**
 *
 * @author Manuel Schneider <schneidm@informatik.uni-freiburg.de>
 *
 */
public class SelectStatement {

	private boolean distinct = false;
	private String from = null;
	private String projection = null;
	private ArrayList<String> joins = new ArrayList<>();
	private String where = null;
	private String groupby = null;
	private String having = null;
	private String limit = null;

	public static SelectStatement createNew(){
		return new SelectStatement();
	}
	
	public SelectStatement distinct() {
		this.distinct = true;
		return this;
	}

	public SelectStatement addProjection(final String projection) {
		if (this.projection == null)
			this.projection = projection;
		else
			this.projection += String.format(",\n\t%s", projection);
		return this;
	}

	public SelectStatement from(final String from) {
		this.from = from;
		return this;
	}

	public SelectStatement crossJoin(final String table, boolean shuffle) {
		this.joins.add(String.format("\nCROSS JOIN %s %s", shuffle ? "/*SHUFFLE*/" : "/*BROADCAST*/", table));
		return this;
	}

	private SelectStatement genericJoin(final String type, final String table, boolean shuffle, final String on) {
		this.joins.add(String.format("\n%s JOIN %s %s ON %s", type, shuffle ? "/*SHUFFLE*/" : "/*BROADCAST*/", table, on));
		return this;
	}

	public SelectStatement join(final String table, final String on, boolean shuffle) {
		genericJoin("INNER", table, shuffle, on);
		return this;
	}

	public SelectStatement leftJoin(final String table, final String on, boolean shuffle) {
		genericJoin("LEFT OUTER", table, shuffle, on);
		return this;
	}

	public SelectStatement rightJoin(final String table, final String on, boolean shuffle) {
		genericJoin("RIGHT OUTER", table, shuffle, on);
		return this;
	}

	public SelectStatement outerJoin(final String table, final String on, boolean shuffle) {
		genericJoin("FULL OUTER", table, shuffle, on);
		return this;
	}

	public SelectStatement leftSemiJoin(final String table, final String on, boolean shuffle) {
		genericJoin("LEFT SEMI", table, shuffle, on);
		return this;
	}

	public SelectStatement rightSemiJoin(final String table, final String on, boolean shuffle) {
		genericJoin("RIGHT SEMI", table, shuffle, on);
		return this;
	}

	public SelectStatement leftAntiJoin(final String table, final String on, boolean shuffle) {
		genericJoin("LEFT ANTI", table, shuffle, on);
		return this;
	}

	public SelectStatement rightAntiJoin(final String table, final String on, boolean shuffle) {
		genericJoin("RIGHT ANTI", table, shuffle, on);
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

	@Override
	public String toString() throws IllegalArgumentException {
		if (projection.isEmpty() || from == null)
			throw new IllegalArgumentException("Projection and table reference must be specified");
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("SELECT%s\n\t%s", distinct ? " DISTINCT": "", projection));
		sb.append(String.format("\nFROM %s", from));
		for (String join : joins)
			sb.append(join);
		if (where != null)
			sb.append(String.format("\nWHERE %s", where));
		if (groupby != null)
			sb.append(String.format("\nGROUP BY %s", groupby));
		if (having != null)
			sb.append(String.format("\nHAVING %s", having));
		if (limit != null)
			sb.append(String.format("\nLIMIT %s", limit));
		return sb.toString();
	}

	public CaseStatement caseBuilder(){
		return new CaseStatement();
	}

	public class CaseStatement
	{
		/*
		 * CASE a WHEN b THEN c [WHEN d THEN e]... [ELSE f] END
		 */
		String expr;
		ArrayList<String> whens = new ArrayList<>();
		ArrayList<String> thens = new ArrayList<>();
		String elseStmt;


		public void addCase(String when, String then){
			whens.add(when);
			thens.add(then);
		}

		public void elseStmt(String elseStmt){
			this.elseStmt=elseStmt;
		}


		@Override
		public String toString() throws IllegalArgumentException {
			if (whens.size()<1)
				throw new IllegalArgumentException("At least one statement is necessary");
			StringBuilder sb = new StringBuilder();
			sb.append(String.format("CASE %s", expr));
			for (int i = 0; i < whens.size(); ++i)
				sb.append(String.format("\nWHEN %s THEN %s", whens.get(i), thens.get(i)));
			sb.append(String.format("\nELSE %s", elseStmt));
			return sb.toString();
		}
	}
}
