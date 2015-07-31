package sparql2impala.op;

import java.util.ArrayList;

import sparql2impala.ImpalaOpVisitor;
import sparql2impala.Tags;
import sparql2impala.sql.SQLStatement;
import sparql2impala.sql.Select;
import sparql2impala.sql.Union;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;

/**
 * 
 * @author Antony Neu
 */
public class ImpalaUnion extends ImpalaOp2 {

	private final OpUnion opUnion;

	public ImpalaUnion(OpUnion _opUnion, ImpalaOp _leftOp, ImpalaOp _rightOp,
			PrefixMapping _prefixes) {
		super(_leftOp, _rightOp, _prefixes);
		opUnion = _opUnion;
		resultName = Tags.UNION;
	}

	public SQLStatement translate(String _resultName, SQLStatement left, SQLStatement right) {
		resultName = _resultName;

		// make column schema equal
		ArrayList<String> onlyLeftVars = ((ImpalaBase) leftOp).getVarsOnlnyInOp((ImpalaBase)rightOp);
		for(String col : onlyLeftVars){
			right.addSelector(col, new String[]{ "null"});
		}
		ArrayList<String> onlyRightVars = ((ImpalaBase) rightOp).getVarsOnlnyInOp((ImpalaBase)leftOp);
		for(String col : onlyRightVars){
			left.addSelector(col, new String[]{ "null"});
		}
		
		// union of schema
		leftOp.getSchema().putAll(rightOp.getSchema());
		resultSchema = leftOp.getSchema();
		
		return new Union(getResultName(), left, right );
	}

	@Override
	public void visit(ImpalaOpVisitor impalaOpVisitor) {
		impalaOpVisitor.visit(this);
	}

}
