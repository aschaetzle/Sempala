package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import com.hp.hpl.jena.shared.PrefixMapping;

import de.uni_freiburg.informatik.dbis.sempala.translator.ImpalaOpVisitor;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;


/**
 *
 * @author Antony Neu
 */



public class ImpalaConditional extends ImpalaOp2 {

	protected ImpalaConditional(ImpalaOp _leftOp, ImpalaOp _rightOp,
			PrefixMapping _prefixes) {
		super(_leftOp, _rightOp, _prefixes);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void visit(ImpalaOpVisitor impalaOpVisitor) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public SQLStatement translate(String name, SQLStatement left,
			SQLStatement right) {
		// TODO Auto-generated method stub
		return null;
	}

    
    
}


