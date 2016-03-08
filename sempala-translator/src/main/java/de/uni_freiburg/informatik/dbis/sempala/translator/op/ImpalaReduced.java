package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import org.apache.jena.atlas.lib.NotImplemented;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;

import de.uni_freiburg.informatik.dbis.sempala.translator.ImpalaOpVisitor;
import de.uni_freiburg.informatik.dbis.sempala.translator.Tags;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;

/**
 * 
 * @author Antony Neu
 */
public class ImpalaReduced extends ImpalaOp1 {

	// TODO: Proper implementation
	@SuppressWarnings("unused")
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
