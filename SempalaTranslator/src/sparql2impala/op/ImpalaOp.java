package sparql2impala.op;

import java.util.ArrayList;
import java.util.Map;

import sparql2impala.ImpalaOpVisitor;

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
