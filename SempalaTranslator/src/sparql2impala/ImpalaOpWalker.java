package sparql2impala;

import java.util.Iterator;

import sparql2impala.op.ImpalaOp;
import sparql2impala.op.ImpalaOp1;
import sparql2impala.op.ImpalaOp2;
import sparql2impala.op.ImpalaOpN;

/**
 * Applies a given ImpalaOpVisitor to all operators in the tree Can walk through
 * the tree bottom up or top down.
 * 
 * @author Antony Neu
 * @see ImpalaOpVisitor.Impala.ImpalaOpVisitor
 */
public class ImpalaOpWalker extends ImpalaOpVisitorByType {

	// Visitor to be applied to all operators in the tree below the given
	// operator
	private final ImpalaOpVisitor visitor;
	// Walk through tree bottom up or top down
	private final boolean topDown;

	/**
	 * Private constructor, initialization using factory functions.
	 * 
	 * @param visitor
	 *            ImpalaOpVisitor to be applied
	 * @param topDown
	 *            true - top down, false - bottom up
	 * @see #walkBottomUp(ImpalaOpVisitor.Impala.ImpalaOpVisitor,
	 *      Impalasparql.Impala.op.ImpalaOp)
	 * @see #walkTopDown(ImpalaOpVisitor.Impala.ImpalaOpVisitor,
	 *      Impalasparql.Impala.op.ImpalaOp)
	 */
	private ImpalaOpWalker(ImpalaOpVisitor visitor, boolean topDown) {
		this.visitor = visitor;
		this.topDown = topDown;
	}

	/**
	 * Apply a given ImpalaOpVisitor to all operators in a ImpalaOp tree walking top
	 * down.
	 * 
	 * @param visitor
	 *            ImpalaOpVisitor to be applied
	 * @param op
	 *            Root of ImpalaOp tree
	 * @see ImpalaOpVisitor.Impala.ImpalaOpVisitor
	 */
	public static void walkTopDown(ImpalaOpVisitor visitor, ImpalaOp op) {
		op.visit(new ImpalaOpWalker(visitor, true));
	}

	/**
	 * Apply a given ImpalaOpVisitor to all operators in a ImpalaOp tree walking
	 * bottom up.
	 * 
	 * @param visitor
	 *            ImpalaOpVisitor to be applied
	 * @param op
	 *            Root of ImpalaOp tree
	 * @see ImpalaOpVisitor.Impala.ImpalaOpVisitor
	 */
	public static void walkBottomUp(ImpalaOpVisitor visitor, ImpalaOp op) {
		op.visit(new ImpalaOpWalker(visitor, false));
	}

	/**
	 * Visit leef operator with no sub operators.
	 * 
	 * @param op
	 */
	@Override
	protected void visit0(ImpalaOp op) {
		op.visit(visitor);
	}

	/**
	 * Visit operator with 1 sub operator.
	 * 
	 * @param op
	 */
	@Override
	protected void visit1(ImpalaOp1 op) {
		if (topDown)
			op.visit(visitor);
		if (op.getSubOp() != null)
			op.getSubOp().visit(this);
		else
			logger.warn("Sub operator is missing in " + op.getResultName());
		if (!topDown)
			op.visit(visitor);
	}

	/**
	 * Visit operator with 2 sub operator.
	 * 
	 * @param op
	 */
	@Override
	protected void visit2(ImpalaOp2 op) {
		if (topDown)
			op.visit(visitor);
		if (op.getLeft() != null)
			op.getLeft().visit(this);
		else
			logger.warn("Left sub operator is missing in " + op.getResultName());
		if (op.getRight() != null)
			op.getRight().visit(this);
		else
			logger.warn("Right sub operator is missing in " + op.getResultName());
		if (!topDown)
			op.visit(visitor);
	}

	/**
	 * Visit operator with N sub operator.
	 * 
	 * @param op
	 */
	@Override
	protected void visitN(ImpalaOpN op) {
		if (topDown)
			op.visit(visitor);
		for (Iterator<ImpalaOp> iter = op.iterator(); iter.hasNext();) {
			ImpalaOp sub = iter.next();
			sub.visit(this);
		}
		if (!topDown)
			op.visit(visitor);
	}

}
