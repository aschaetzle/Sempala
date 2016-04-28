package de.uni_freiburg.informatik.dbis.sempala.translator.sparql;

import java.util.Stack;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;

import de.uni_freiburg.informatik.dbis.sempala.translator.Format;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaBgpPropertyTable;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaBgpSingleTable;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaDistinct;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaFilter;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaJoin;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaLeftJoin;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaOp;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaOrder;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaProject;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaReduced;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaSequence;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaSlice;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaUnion;

/**
 *
 * @author Antony Neu
 */
public class AlgebraTransformer extends OpVisitorBase {

	private final Stack<ImpalaOp> stack;
	private final PrefixMapping prefixes;
	private final Format format;


	public AlgebraTransformer(PrefixMapping prefixes, Format format) {
		stack = new Stack<ImpalaOp>();
		this.prefixes = prefixes;
		this.format = format;
	}

	public ImpalaOp transform(Op op) {
		AlgebraWalker.walkBottomUp(this, op);
		return stack.pop();
	}

	@Override
	public void visit(OpBGP opBGP) {
		switch (format) {
		case PROPERTYTABLE:
			stack.push(new ImpalaBgpPropertyTable(opBGP, prefixes));
			break;
		case SINGLETABLE:
			stack.push(new ImpalaBgpSingleTable(opBGP, prefixes));
			break;
		}
	}

	@Override
	public void visit(OpFilter opFilter) {
		ImpalaOp subOp = stack.pop();
		stack.push(new ImpalaFilter(opFilter, subOp, prefixes));
	}

	@Override
	public void visit(OpJoin opJoin) {
		ImpalaOp rightOp = stack.pop();
		ImpalaOp leftOp = stack.pop();
		stack.push(new ImpalaJoin(opJoin, leftOp, rightOp, prefixes));
	}

	@Override
	public void visit(OpSequence opSequence) {
		ImpalaSequence ImpalaSequence = new ImpalaSequence(opSequence, prefixes);
		for(int i=0; i<opSequence.size(); i++) {
			ImpalaSequence.add(0,stack.pop());
		}
		stack.push(ImpalaSequence);
	}

	@Override
	public void visit(OpLeftJoin opLeftJoin) {
		ImpalaOp rightOp = stack.pop();
		ImpalaOp leftOp = stack.pop();
		stack.push(new ImpalaLeftJoin(opLeftJoin, leftOp, rightOp, prefixes));
	}

	/*
	@Override
	public void visit(OpConditional opConditional) {
		ImpalaOp rightOp = stack.pop();
		ImpalaOp leftOp = stack.pop();
		stack.push(new ImpalaConditional(opConditional, leftOp, rightOp, prefixes));
	} */

	@Override
	public void visit(OpUnion opUnion) {
		ImpalaOp rightOp = stack.pop();
		ImpalaOp leftOp = stack.pop();
		stack.push(new ImpalaUnion(opUnion, leftOp, rightOp, prefixes));
	}

	@Override
	public void visit(OpProject opProject) {
		ImpalaOp subOp = stack.pop();
		stack.push(new ImpalaProject(opProject, subOp, prefixes));
	}

	@Override
	public void visit(OpDistinct opDistinct) {
		ImpalaOp subOp = stack.pop();
		stack.push(new ImpalaDistinct(opDistinct, subOp, prefixes));
	}

	@Override
	public void visit(OpOrder opOrder) {
		ImpalaOp subOp = stack.pop();
		stack.push(new ImpalaOrder(opOrder, subOp, prefixes));
	}

	@Override
	public void visit(OpSlice opSlice) {
		ImpalaOp subOp = stack.pop();
		// change order of tree nodes
		if(subOp instanceof ImpalaProject){
			ImpalaSlice slice = new ImpalaSlice(opSlice, ((ImpalaProject) subOp).getSubOp(), prefixes);
			ImpalaProject project = new ImpalaProject(((ImpalaProject) subOp).getOpProject(), slice, prefixes);
			stack.push(project);
		} else {
			stack.push(new ImpalaSlice(opSlice, subOp, prefixes));
		}
	}

	@Override
	public void visit(OpReduced opReduced) {
		ImpalaOp subOp = stack.pop();
		stack.push(new ImpalaReduced(opReduced, subOp, prefixes));
	}

}
