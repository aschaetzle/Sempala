package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import java.util.Map;

import de.uni_freiburg.informatik.dbis.sempala.translator.ImpalaOpVisitor;

/**
 *
 * @author Antony Neu
 */
public interface ImpalaOp {
	public Map<String, String[]> getSchema();
	public String getResultName();
	public void setResultName(String _resultName);
	public boolean getExpandMode();
	public void setExpandMode(boolean _expandPrefixes);
	public void visit(ImpalaOpVisitor impalaOpVisitor);

}
