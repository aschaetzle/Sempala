package de.uni_freiburg.informatik.dbis.sempala.translator.sql;

import java.util.HashMap;
import java.util.Map;

import org.apache.jena.atlas.lib.NotImplemented;

public class Union extends SQLStatement {

	private SQLStatement left;
	private SQLStatement right;
	
	public Union(String tablename, SQLStatement left, SQLStatement right) {
		super(tablename);
		this.left = left;
		this.right = right;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("(");
		sb.append(left.toString());
		sb.append(" UNION ");
		sb.append(right.toString());
		sb.append(")");
		return sb.toString();
	}

	// TODO
	@Override
	public void addSelector(String alias, String[] selector) {
		left.addSelector(alias, selector);
		right.addSelector(alias, selector);
	}

	
	
	@Override
	public HashMap<String, String[]> getSelectors() {
		return left.getSelectors();
	}

	@Override
	public void addWhereConjunction(String where) {
		left.addWhereConjunction(where);
		right.addWhereConjunction(where);
	}

	@Override
	public void addOrder(String byColumn) {
		left.addOrder(byColumn);
		right.addOrder(byColumn);
		
	}

	@Override
	public void updateSelection(Map<String, String[]> resultSchema) {
		left.updateSelection(resultSchema);
		right.updateSelection(resultSchema);
		
	}

	@Override
	public void removeNullFilters() {
		left.removeNullFilters();
		right.removeNullFilters();	
	}

	@Override
	public void addLimit(int i) {
		left.addLimit(i);
		right.addLimit(i);
	}

	@Override
	public boolean addOffset(int i) {
		// TODO Auto-generated method stub
		throw new NotImplemented();
	}

	@Override
	public String getOrder() {
		return left.getOrder();
	}



}
