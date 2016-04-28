package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;

import de.uni_freiburg.informatik.dbis.sempala.translator.ImpalaOpVisitor;
import de.uni_freiburg.informatik.dbis.sempala.translator.Tags;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;

/**
 *
 * @author Antony Neu
 */
public abstract class ImpalaBGP extends ImpalaOp0 {

	protected final OpBGP opBGP;
	protected int tableNumber = 0;

	public ImpalaBGP(OpBGP opBGP, PrefixMapping prefixes) {
		super(prefixes);
		this.opBGP = opBGP;
		this.resultName = Tags.BGP;
	}

	public abstract SQLStatement translate(String resultName);

	@Override
	public void visit(ImpalaOpVisitor impalaOpVisitor) {
		impalaOpVisitor.visit(this);
	}

}