package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import java.util.ArrayList;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;

import de.uni_freiburg.informatik.dbis.sempala.translator.ImpalaOpVisitor;
import de.uni_freiburg.informatik.dbis.sempala.translator.Tags;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.Select;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.Union;

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
