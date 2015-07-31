package sparql2impala.op;

import sparql2impala.ImpalaOpVisitor;
import sparql2impala.Tags;
import sparql2impala.sql.SQLStatement;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;

/**
 * 
 * @author Antony Neu
 */
public class ImpalaDistinct extends ImpalaOp1 {

	private final OpDistinct opDistinct;

	public ImpalaDistinct(OpDistinct _opDistinct, ImpalaOp _subOp,
			PrefixMapping _prefixes) {
		super(_subOp, _prefixes);
		opDistinct = _opDistinct;
		resultName = Tags.DISTINCT;
	}

	@Override
	public void visit(ImpalaOpVisitor impalaOpVisitor) {
		impalaOpVisitor.visit(this);
	}

	@Override
	public SQLStatement translate(String name, SQLStatement child) {
		// TODO Auto-generated method stub
		return null;
	}

}
