package de.uni_freiburg.informatik.dbis.sempala.translator.sql;

import java.util.HashMap;
import java.util.Map;

public class ComplexSelect extends SQLStatement {

	protected String from = "";
	protected String where = "";
	protected String order = "";
	private int limit = -1;
	private int offset = -1;
	HashMap<String, String[]> selection = new HashMap<String, String[]>();
	HashMap<String, String> inverted_selection = new HashMap<String, String>();
	HashMap<String, Boolean> is_complex_column = new HashMap<String, Boolean>();
	private boolean contains_complex_vars = false;

	/*
	 * Subset of schema
	 */

	public ComplexSelect(String tablename) {
		super(tablename);
	}

	@Override
	public void addSelector(String alias, String[] selector) {
		selection.put(alias, selector);
		if(selector.length > 1){
			inverted_selection.put(selector[0] + "." + selector[1], alias);
		} else {
			inverted_selection.put(selector[0], alias);
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
		if (contains_complex_vars) {
			// check if it contains a property
			for (String key : is_complex_column.keySet()) {
				int pos = condition.indexOf(key);
				// the property is present in the condition
				if (pos > -1) {
					// the property is also of complex type
					if (is_complex_column.get(key)) {
						condition = condition.replaceAll(key, "subT_" + inverted_selection.get(key) + ".ITEM ");
					} else { // if not, add the reference to t1 (the first reference to the table)
						condition = condition.substring(0, pos) + "t1." + condition.substring(pos);
					}
				}
			} 
		}
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
		if (isDistinct)
			sb.append(" DISTINCT");
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
						sb.append(" " + selector[0] + "." + selector[1] + " AS " + "\"" + key + "\"");
					} else {
						sb.append(" " + selector[0] + " AS " + "\"" + key + "\"");
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

	// needed if some of the variables are complex
	public String complexSelect() {
		StringBuilder sb = new StringBuilder("");
		sb.append("(\nSELECT");
		if (isDistinct)
			sb.append(" DISTINCT");
		
		/**
		 *  Impala does not allow to select with '*' complex columns
		 *  Therefore, all the columns are explicitly listed
		 */
		if (selection.size() == 0){
			int refName = 0;
			for(String key : is_complex_column.keySet())
				this.addSelector("v_" + String.valueOf(refName++), new String[]{key});
		}
			
		boolean first = true;
		for (String key : selection.keySet()) {
			String[] selector = selection.get(key);
			if (selector != null) {
				if (first) {
					first = false;
				} else {
					sb.append(",");
				}
				if (!is_complex_column.get(selector[0])) {
					if (selector.length > 1) {
						sb.append(" t1." + selector[0] + "." + selector[1] + " AS " + "\"" + key + "\"");
					} else {
						sb.append(" t1." + selector[0] + " AS " + "\"" + key + "\"");
					}
				} else {
					// distinguish the complex properties
					String subTableName = "subT_" + key;

					sb.append(" " + subTableName + ".ITEM AS " + "\"" + key + "\"");

				}
			}
		}
		
		sb.append("\nFROM ");
		sb.append(from  + " t1 ");
		sb.append(" \n INNER JOIN ");
		first = true;
		for (String key : selection.keySet()) {
			String[] selector = selection.get(key);
			if (selector != null && is_complex_column.get(selector[0])) {
				if (first) {
					first = false;
				} else {
					sb.append(",");
				}

				if (selector.length > 1) {
					sb.append(" t1." + selector[0] + "." + selector[1] + " subT_" + key);
				} else {
					sb.append(" t1." + selector[0] + " subT_" + key);
				}

			}
		}
		if (!this.where.equals("")) {
			sb.append(" \nWHERE ");
			this.removeNullFilters();
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

		// if there are complex properties -> complex select is needed
		
		if (selection.size() == 0)
			contains_complex_vars = true;
		else {
			for (String key : selection.keySet()) {
				String[] selector = selection.get(key);
				if (is_complex_column.get(selector[0])) {
					contains_complex_vars = true;
					break;
				}
			}
		}
		
		if (contains_complex_vars)
			return complexSelect();
		else
			return simpleSelect();
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
