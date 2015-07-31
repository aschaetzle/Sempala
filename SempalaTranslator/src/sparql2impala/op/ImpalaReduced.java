package sparql2impala.op;

import org.apache.jena.atlas.lib.NotImplemented;

import sparql2impala.ImpalaOpVisitor;
import sparql2impala.Tags;
import sparql2impala.sql.SQLStatement;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;

/**
 * 
 * @author Antony Neu
 */
public class ImpalaReduced extends ImpalaOp1 {

	private final OpReduced opReduced;

	public ImpalaReduced(OpReduced _opReduced, ImpalaOp _subOp,
			PrefixMapping _prefixes) {
		super(_subOp, _prefixes);
		opReduced = _opReduced;
		resultName = Tags.REDUCED;
	}



	@Override
	public void visit(ImpalaOpVisitor impalaOpVisitor) {
		impalaOpVisitor.visit(this);
	}



	@Override
	public SQLStatement translate(String name, SQLStatement child) {
		// TODO Auto-generated method stub
		throw new NotImplemented();
	}

}
