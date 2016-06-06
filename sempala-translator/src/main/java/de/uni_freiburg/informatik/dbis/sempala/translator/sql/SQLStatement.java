package de.uni_freiburg.informatik.dbis.sempala.translator.sql;

import java.util.HashMap;
import java.util.Map;

public abstract class SQLStatement {

	protected String statementName;
	protected boolean isDistinct = false;

	public String getName() {
		return statementName;
	}

	public SQLStatement(String name) {
		this.statementName = name;
	}

	public abstract void addWhereConjunction(String where);

	public abstract void addOrder(String byColumn);

	public abstract String getOrder();

	@Override
	public abstract String toString();

	/**
	 *
	 * @return String represantation with item name, e.g. (SELECT ...) table1.
	 */
	public String toNamedString() {
		return this.toString() + " " + statementName;
	}

	public abstract void addSelector(String alias, String[] selector);

	public abstract HashMap<String, String[]> getSelectors();

	public abstract void updateSelection(Map<String, String[]> resultSchema);


	public abstract void addLimit(int i);
	public abstract boolean addOffset(int i);

	public void setDistinct(boolean b) {
		this.isDistinct = b;
	}

	public abstract void removeNullFilters();
}
