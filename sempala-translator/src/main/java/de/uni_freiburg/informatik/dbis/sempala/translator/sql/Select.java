package de.uni_freiburg.informatik.dbis.sempala.translator.sql;

import java.util.HashMap;
import java.util.Map;

public class Select extends SQLStatement {

	/*
	 * Subset of schema
	 */
	HashMap<String, String[]> selection = new HashMap<String, String[]>();

	public Select(String tablename) {
		super(tablename);
	}

	private int limit = -1;
	private int offset = -1;


	public void addSelector(String alias, String[] selector) {
		selection.put(alias, selector);
	}

	protected String order = "";

	protected String from = "";

	public void appendToFrom(String s) {
		from += s;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	protected String where = "";

	public void addWhereConjunction(String condition) {
		if (where.equals("")) {
			where += condition;
		} else {
			where += " AND " + condition;
		}
	}

	public void addOrder(String by) {
		this.order = by;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder("");
		sb.append("(SELECT ");
		//if(isDistinct)
			sb.append(" DISTINCT ");
		if (selection.size() == 0) {
			sb.append("* ");
		} else {
			boolean first = true;
			for (String key : selection.keySet()) {
				String[] selector = selection.get(key);
				if (first) {
					first = false;
				} else {
					sb.append(", ");
				}
				if (selector.length > 1) {
					sb.append(selector[0]+"." + selector[1]+  " AS " + "\"" + key + "\" ");
				} else {
					sb.append(selector[0]+  " AS " + "\"" + key + "\" ");
				}
			}
		}
		sb.append("\n FROM ");
		// pretty formating
		if (from.length() > 10)
			sb.append("\n");
		sb.append(from);
		if (!this.where.equals("")) {
			sb.append(" \n WHERE ");
			// pretty formating
			if (where.length() > 10)
				sb.append("\n");
			sb.append(where);
		}
		if (!this.order.equals("")) {
			sb.append("\n ORDER BY ");
			sb.append(order);
		}
		if(this.limit != -1){
			sb.append("\n LIMIT ");
			sb.append(this.limit);
		}
		if(this.offset != -1){
			sb.append("\n OFFSET ");
			sb.append(this.offset);
		}
		sb.append(")");
		return sb.toString();
	}

	@Override
	public HashMap<String, String[]> getSelectors() {
		return this.selection;
	}

	@Override
	public void updateSelection(Map<String, String[]> resultSchema) {
		for (String key : resultSchema.keySet()) {
			if (this.selection.containsKey(key)) {
				String[] entry;
				if(selection.get(key).length >1){
					entry = new String[]{selection.get(key)[0], resultSchema.get(key)[1]};
				} else{
					entry = new String[]{ resultSchema.get(key)[0]};
				}
				this.selection.put(key, entry);
			}
		}

	}

	@Override
	public void removeNullFilters() {
		String[] filters =  where.split(" AND ");
		for(int i = 0; i < filters.length; i++){
			if(filters[i].toLowerCase().contains("is not null")){
				filters[i] = "";
			}
		}
		this.where = "";
		for(String filter : filters){
			if(!filter.equals(""))
			this.addWhereConjunction(filter);
		}

	}

	public void setName(String string) {
		this.statementName = string;

	}


	public String getName(){
		return this.statementName;
	}

	@Override
	public void addLimit(int i) {
		this.limit = i;

	}

	@Override
	public boolean addOffset(int i) {
		if(!this.order.equals("")){
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
