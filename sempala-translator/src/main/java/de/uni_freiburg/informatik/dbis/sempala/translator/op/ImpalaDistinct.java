package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;

import de.uni_freiburg.informatik.dbis.sempala.translator.ImpalaOpVisitor;
import de.uni_freiburg.informatik.dbis.sempala.translator.Tags;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;

/**
 * 
 * @author Antony Neu
 */
public class ImpalaDistinct extends ImpalaOp1 {
	
	// TODO: Proper implementation
	@SuppressWarnings("unused")
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
