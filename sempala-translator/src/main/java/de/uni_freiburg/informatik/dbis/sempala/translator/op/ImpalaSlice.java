package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;

import de.uni_freiburg.informatik.dbis.sempala.translator.ImpalaOpVisitor;
import de.uni_freiburg.informatik.dbis.sempala.translator.Tags;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;

/**
 *
 * @author Antony Neu
 */
public class ImpalaSlice extends ImpalaOp1 {

	private final OpSlice opSlice;

	public ImpalaSlice(OpSlice _opSlice, ImpalaOp _subOp,
			PrefixMapping _prefixes) {
		super(_subOp, _prefixes);
		opSlice = _opSlice;
		resultName = Tags.SLICE;
	}

	@Override
	public SQLStatement translate(String _resultName, SQLStatement child) {
		resultName = subOp.getResultName();
		resultSchema = subOp.getSchema();

		if (opSlice.getStart() > 0) {
			child.addOffset((int) opSlice.getStart());
		}
		if (opSlice.getLength() > 0) {
			child.addLimit((int) opSlice.getLength());
		}

		return child;
	}

	@Override
	public void visit(ImpalaOpVisitor impalaOpVisitor) {
		impalaOpVisitor.visit(this);
	}

}
