package de.uni_freiburg.informatik.dbis.sempala.translator.sql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class Join extends SQLStatement {

	private SQLStatement left;
	private List<SQLStatement> rights;
	private Select wrapper;
	private List<String> onStrings;
	private JoinType type = JoinType.INNER;

	public Join(String tablename, SQLStatement left, List<SQLStatement> rights,
			List<String> onStrings, JoinType type) {
		super(tablename);
		this.left = left;
		this.rights = rights;
		this.type = type;
		this.onStrings = onStrings;

		// add selectors only once
		HashSet<String> added = new HashSet<String>();
		this.wrapper = new Select(tablename);
		for (String key: left.getSelectors().keySet()) {
//			String[] selector = left.getSelectors().get(key);
//			String name = "";
//			if(selector.length > 1){
//				name = selector[1];
//			} else{
//				name = selector[0];
//			}
			if (!added.contains(key)) {
				added.add(key);
				wrapper.addSelector(key, new String[] { left.getName() ,key });
			}
		}
		for (SQLStatement right : rights) {
			for (String key: right.getSelectors().keySet()) {
//				String[] selector = right.getSelectors().get(key);
//				String name = "";
//				if(selector.length > 1){
//					name = selector[1];
//				} else{
//					name = selector[0];
//				}
				if (!added.contains(key)) {
					added.add(key);
					wrapper.addSelector(key, new String[] { right.getName(), key });
				}
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("");
		sb.append(left.toNamedString());
		for (int i = 0; i < rights.size(); i++) {
			SQLStatement right = rights.get(i);
			String onString = onStrings.get(i);
			if(onString.equals("") || this.type == JoinType.CROSS){
				sb.append(" CROSS JOIN ");
			}
			else if(this.type == JoinType.LEFT){
			sb.append(" LEFT JOIN ");
			} else{
				sb.append(" JOIN ");	
			}
			sb.append(right.toNamedString());
			if(!onString.equals("") && this.type != JoinType.CROSS){
			sb.append(" ON(");
			sb.append(onString);
			sb.append(")");
			} else if(!onString.equals("") && this.type == JoinType.CROSS){
				wrapper.addWhereConjunction(onString);
			}
		}
		wrapper.setFrom(sb.toString());

		return wrapper.toString();
	}

	// TODO
	@Override
	public void addSelector(String alias, String[] selector) {
		wrapper.addSelector(alias, selector);

	}

	@Override
	public HashMap<String, String[]> getSelectors() {
		return wrapper.getSelectors();
	}

	@Override
	public void addWhereConjunction(String where) {
		wrapper.addWhereConjunction(where);

	}

	@Override
	public void addOrder(String byColumn) {
		wrapper.addOrder(byColumn);
	}

	@Override
	public void updateSelection(Map<String, String[]> resultSchema) {
		wrapper.updateSelection(resultSchema);

	}

	@Override
	public void removeNullFilters() {
		wrapper.removeNullFilters();
//		left.removeNullFilters();
//		for(SQLStatement right: rights){
//			right.removeNullFilters();
//		}
	}

	@Override
	public void addLimit(int i) {
		wrapper.addLimit(i);

	}

	@Override
	public boolean addOffset(int i) {
		return wrapper.addOffset(i);
	}

	@Override
	public String getOrder() {
		return this.wrapper.getOrder();
	}



}
