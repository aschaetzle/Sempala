package de.uni_freiburg.informatik.dbis.sempala.translator.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import de.uni_freiburg.informatik.dbis.sempala.translator.ComplexPropertyTableColumns;

/**
 * A Spark SQL select statement. Because the complex property table has columns
 * which can be of complex type (<array>), this select works if such a column is
 * part of the select query (included in the selected columns or part of a
 * join). The differences are:
 * 
 * 1) if a complex column is a part of a join it is flatten with "LATERAL VIEW
 * EXPLODE" statement 2) if a complex column is a part of a selection it is
 * flatten with "LATERAL VIEW EXPLODE" statement
 * 
 * @author Polina Koleva
 *
 */
public class SparkComplexSelect extends SQLStatement {

	public SparkComplexSelect(String name) {
		super(name);
	}

	protected String from = "";
	protected String where = "";
	protected String order = "";
	private int limit = -1;
	private int offset = -1;

	// <alias, selectors>
	HashMap<String, String[]> selection = new HashMap<String, String[]>();

	// <complex_propery, view name>
	private HashMap<String, String> viewProperties = new HashMap<String, String>();

	// <column name, is the column complex>
	HashMap<String, Boolean> is_complex_column = new HashMap<String, Boolean>();

	// properties that need to be considered in case of cross joins
	public HashMap<String, ArrayList<String>> crossProperties = new HashMap<String, ArrayList<String>>();

	// if there is a complex column which is part of a join or is part of the
	// selected columns
	private boolean hasComplexColumn = false;

	@Override
	public void addSelector(String alias, String[] selector) {
		// if we join on a complex property or a complex property is part of
		// selected columns
		// we need to use complexSelected and flatten these complex properties
		// NOTE: because if we join on a complex property it will be always part
		// of the selected columns, there is no need to check join variables
		if (is_complex_column.get(selector[0])) {
			if (hasComplexColumn == false) {
				hasComplexColumn = true;
			}
			int viewPropSize = viewProperties.size();
			String viewName = "lve_" + (viewPropSize + 1);
			viewProperties.put(selector[0], viewName);
			selection.put(alias, new String[] { viewName + "_" + selector[0] });
		} else {
			selection.put(alias, selector);
		}
	}

	public void appendToFrom(String s) {
		from += s;
	}

	public void setComplexColumns(HashMap<String, Boolean> is_complex) {
		is_complex_column = is_complex;
	}

	public void setFrom(String from) {
		this.from = from.trim();
	}

	@Override
	public void addWhereConjunction(String condition) {
		if (where.equals("")) {
			where += condition;
		} else {
			where += "\n  AND " + condition;
		}
	}

	@Override
	public void addOrder(String by) {
		this.order = by;
	}

	public String simpleSelect() {
		StringBuilder sb = new StringBuilder("");
		sb.append("(\nSELECT");
		if (isDistinct) {
			sb.append(" DISTINCT");
		}
		if (selection.size() == 0) {
			sb.append(" *");
		} else {
			boolean first = true;
			for (String key : selection.keySet()) {
				String[] selector = selection.get(key);
				if (selector != null) {
					if (first) {
						first = false;
					} else {
						sb.append(",");
					}
					if (selector.length > 1) {
						sb.append(" " + selector[0] + "." + selector[1] + " AS " + key);
					} else {
						sb.append(" " + selector[0] + " AS " + key);
					}
				}
			}
		}
		sb.append("\nFROM ");
		sb.append(from.replaceAll("\n", "\n  "));
		if (!this.where.equals("")) {
			sb.append(" \nWHERE ");
			sb.append(where);
		}
		if (!this.order.equals("")) {
			sb.append("\nORDER BY ");
			sb.append(order);
		}
		if (this.limit != -1) {
			sb.append("\nLIMIT ");
			sb.append(this.limit);
		}
		if (this.offset != -1) {
			sb.append("\nOFFSET ");
			sb.append(this.offset);
		}
		sb.append("\n)");
		return sb.toString();
	}

	// needed if some of the variables part of the join are complex
	public String complexSelect() {
		StringBuilder sb = new StringBuilder("");
		sb.append("(\nSELECT");
		if (isDistinct) {
			sb.append(" DISTINCT");
		}
		boolean first = true;
		for (String key : selection.keySet()) {
			String[] selector = selection.get(key);
			if (selector != null) {
				if (first) {
					first = false;
					sb.append(" ");
				} else {
					sb.append(", ");
				}
				if (selector.length > 1) {
					sb.append(selector[0] + "." + selector[1] + " AS " + key);
				} else {
					sb.append(selector[0] + " AS " + key);
				}
			}
		}

		sb.append("\nFROM ");
		sb.append(from.replaceAll("\n", "\n  "));
		// add Lateral views
		for (Map.Entry<String, String> entry : viewProperties.entrySet()) {
			String complexPropertyName = entry.getKey();
			String viewName = entry.getValue();
			sb.append("\nLATERAL VIEW EXPLODE(" + from.replaceAll("\n", "\n  ") + "." + complexPropertyName + ")" + " "
					+ viewName + " AS " + (viewName + "_" + complexPropertyName));
		}
		
		// add also additional cross joins on the same predicate if are needed
		for (String predicate : crossProperties.keySet()) {
			ArrayList<String> selectors = crossProperties.get(predicate);
			for (int i = 0; i < selectors.size(); i++) {
				String condition = "array_contains(" + predicate + ", '" + selectors.get(i) + "')";
				// and add it also to the where condition
				this.where += "\nAND " + condition;
			}
		}

		if (!this.where.equals("")) {
			sb.append(" \nWHERE ");
			sb.append(where);
		}
		if (!this.order.equals("")) {
			sb.append("\nORDER BY ");
			sb.append(order);
		}
		if (this.limit != -1) {
			sb.append("\nLIMIT ");
			sb.append(this.limit);
		}
		if (this.offset != -1) {
			sb.append("\nOFFSET ");
			sb.append(this.offset);
		}
		sb.append("\n)");
		return sb.toString();

	}

	@Override
	public String toString() {
		// if has join on complex property or selection of a complex property
		// use the select that flattens all complex properties included in a
		// join or selected
		if (hasComplexColumn) {
			return complexSelect();
		} else {
			return simpleSelect();
		}
	}

	@Override
	public HashMap<String, String[]> getSelectors() {
		return this.selection;
	}

	@Override
	public void updateSelection(Map<String, String[]> resultSchema) {
		for (String key : resultSchema.keySet()) {
			// should this be not containsKey?
			if (this.selection.containsKey(key)) {
				String[] entry;
				if (selection.get(key).length > 1) {
					entry = new String[] { selection.get(key)[0], resultSchema.get(key)[1] };
				} else {
					entry = new String[] { resultSchema.get(key)[0] };
				}
				this.selection.put(key, entry);
			}
		}

	}

	@Override
	public void removeNullFilters() {
		String[] filters = where.split(" AND ");
		for (int i = 0; i < filters.length; i++) {
			if (filters[i].toUpperCase().contains("IS NOT NULL")) {
				filters[i] = "";
			}
		}
		this.where = "";
		for (String filter : filters) {
			if (!filter.equals(""))
				this.addWhereConjunction(filter);
		}
	}

	public void setName(String string) {
		this.statementName = string;

	}

	@Override
	public String getName() {
		return this.statementName;
	}

	@Override
	public void addLimit(int i) {
		this.limit = i;

	}

	@Override
	public boolean addOffset(int i) {
		if (!this.order.equals("")) {
			this.offset = i;
			return true;
		}
		return false;
	}

	@Override
	public String getOrder() {
		return this.order;
	}
}
