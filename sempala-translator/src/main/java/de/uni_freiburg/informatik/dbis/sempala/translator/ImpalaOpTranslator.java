package de.uni_freiburg.informatik.dbis.sempala.translator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.jena.atlas.lib.NotImplemented;

import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaBGP;
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
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.Join;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.JoinType;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.JoinUtil;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.Schema;

/**
 * Translates a ImpalaOp Tree into a corresponding Impala SQL program. It walks
 * through the ImpalaOp Tree bottom up and generates the commands for every
 * operator. A SPARQL Algebra Tree must be translated into a corresponding ImpalaOp
 * Tree before using the ImpalaOpTranslator.
 * 
 * @see de.uni_freiburg.informatik.dbis.sempala.translator.sparql.AlgebraTransformer
 */
public class ImpalaOpTranslator extends ImpalaOpVisitorBase {

	// Expand prefixes or not
	private boolean expandPrefixes;
	// Count the occurences of an operator
	private int countBGP, countJoin, countLeftJoin, countUnion, countFilter;

	private Stack<SQLStatement> stack = new Stack<SQLStatement>();

	/**
	 * Constructor of class ImpalaOpTranslator.
	 */
	public ImpalaOpTranslator() {
		countBGP = 0;
		countJoin = 0;
		countLeftJoin = 0;
		countUnion = 0;
		countFilter = 0;
	}

	/**
	 * Translates a ImpalaOp Tree into a corresponding Impala SQL program.
	 * 
	 * @param op
	 *            Root of the ImpalaOp Tree
	 * @param _expandPrefixes
	 *            Expand prefixes used in the original query or not
	 * @return Impala SQL program
	 */
	public String translate(ImpalaOp op, boolean _expandPrefixes) {
		expandPrefixes = _expandPrefixes;
		// Walk through the tree bottom up
		ImpalaOpWalker.walkBottomUp(this, op);
		// put stack top here
		String raw = Tags.QUERY_PREFIX + stack.pop().toString() + " ; "+ Tags.QUERY_SUFFIX;
		return clean(raw);
	}

	private String clean(String raw) {
		StringBuilder sb = new StringBuilder();
		Stack<Character> chars = new Stack<Character>();
		for (Character c : raw.toCharArray()) {
			if (c.equals('\'')) {
				if (!chars.isEmpty() && chars.peek().equals('\'')) {
					chars.pop();
				} else {
					chars.push('\'');
				}
			} else if (!chars.isEmpty() && c.equals('"')) {
				if (chars.peek().equals('"')) {
					chars.pop();
				} else {
					chars.push('"');
				}
			}
			if (c.equals(':')) {
				if (chars.isEmpty()) {
					sb.append('_');
				} else {
					sb.append(':');
				}
			} else {
				sb.append(c);
			}

		}
		return sb.toString();
	}

	/**
	 * Translates a BGP into corresponding Impala SQL commands.
	 * 
	 * @param bgp
	 *            BGP in the ImpalaOp Tree
	 */
	@Override
	public void visit(ImpalaBGP bgp) {
		countBGP++;
		bgp.setExpandMode(expandPrefixes);
		stack.push(bgp.translate(Tags.BGP + countBGP));
	}

	/**
	 * Translates a FILTER into corresponding Impala SQL commands.
	 * 
	 * @param filter
	 *            FILTER in the ImpalaOp Tree
	 */
	@Override
	public void visit(ImpalaFilter filter) {
		countFilter++;
		stack.push(filter.translate(Tags.FILTER + countFilter, stack.pop()));
	}

	/**
	 * Translates a JOIN into corresponding SQL commands.
	 * 
	 * @param join
	 *            JOIN in the ImpalaOp Tree
	 */
	@Override
	public void visit(ImpalaJoin join) {
		countJoin++;
		SQLStatement right = stack.pop();
		SQLStatement left = stack.pop();
		stack.push(join.translate(Tags.JOIN + countJoin, left, right));
	}

	/**
	 * Translates a sequence of JOINs into corresponding Impala SQL Latin commands.
	 * 
	 * @param sequence
	 *            JOIN sequence in the ImpalaOp Tree
	 */
	@Override
	public void visit(ImpalaSequence sequence) {
		Stack<SQLStatement> children_rev = new Stack<SQLStatement>();
		for (int i = 0; i < sequence.size(); i++) {
			children_rev.push(stack.pop());
		}
		List<SQLStatement> children = new ArrayList<SQLStatement>();
		for (int i = 0; i < sequence.size(); i++) {
			children.add(children_rev.pop());
		}

		countJoin++;
		sequence.setResultName(Tags.SEQUENCE + countJoin);

		Map<String, String[]> filterSchema = new HashMap<String, String[]>();
		String tablename = sequence.getResultName();
		List<String> onStrings = new ArrayList<String>();
		for (int i = 1; i < children.size(); i++) {
			Map<String, String[]> leftschema = Schema.shiftToParent(sequence
					.getElements().get(i - 1).getSchema(), sequence
					.getElements().get(i - 1).getResultName());
			Map<String, String[]> rightschema = Schema.shiftToParent(sequence
					.getElements().get(i).getSchema(), sequence.getElements()
					.get(i).getResultName());
			filterSchema.putAll(leftschema);
			filterSchema.putAll(rightschema);
			onStrings.add(JoinUtil.generateConjunction(JoinUtil
					.getOnConditions(leftschema, rightschema)));
		}

		sequence.setSchema(filterSchema);
		Join join = new Join(tablename, children.get(0), children.subList(1,
				children.size()), onStrings, JoinType.natural);

		stack.push(join);
	}

	/**
	 * Translates a LEFTJOIN into corresponding Impala SQL commands.
	 * 
	 * @param impalaLeftJoin
	 *            LEFTJOIN in the ImpalaOp Tree
	 */
	@Override
	public void visit(ImpalaLeftJoin impalaLeftJoin) {

		SQLStatement right = stack.pop();
		SQLStatement left = stack.pop();
		stack.push(impalaLeftJoin.translate(Tags.LEFT_JOIN + countLeftJoin++,
				left, right));
	}

	/**
	 * Translates a LEFTJOIN without Filter (Conditional) into corresponding Impala SQL
	 *  commands.
	 * 
	 * @param impalaConditional
	 *            LEFTJOIN without Filter (Conditional) in the ImpalaOp Tree
	 */

	/*
	 * @Override public void visit(ImpalaConditional impalaConditional) { throw new
	 * NotImplemented(); // countLeftJoin++; // String[] leftJoin =
	 * impalaConditional.translate(Tags.CONDITIONAL // + countLeftJoin); //
	 * prependToScript(leftJoin[0]); // appendToScript(leftJoin[1]); }
	 */

	/**
	 * Translates a UNION into corresponding Impala SQL commands.
	 * 
	 * @param impalaUnion
	 *            UNION in the ImpalaOp Tree
	 */
	@Override
	public void visit(ImpalaUnion union) {
		countUnion++;
		SQLStatement right = stack.pop();
		SQLStatement left = stack.pop();
		SQLStatement s = union.translate(Tags.UNION + countUnion, left, right);
		stack.push(s);
	}

	/**
	 * Translates a PROJECT into corresponding Impala SQL commands.
	 * 
	 * @param impalaProject
	 *            PROJECT in the ImpalaOp Tree
	 */
	@Override
	public void visit(ImpalaProject impalaProject) {
		SQLStatement projection = impalaProject.translate(Tags.PROJECT,
				stack.pop());
		stack.push(projection);
	}

	/**
	 * Translates a DISTINCT into corresponding Impala SQL commands.
	 * 
	 * @param impalaDistinct
	 *            Distinct in the ImpalaOp Tree
	 */
	@Override
	public void visit(ImpalaDistinct distinct) {
		SQLStatement sql = stack.pop();
		sql.setDistinct(true);
		stack.push(sql);
	}

	/**
	 * Translates a REDUCE into corresponding Impala SQL commands.
	 * 
	 * @param impalaReduced
	 *            REDUCE in the ImpalaOp Tree
	 */
	@Override
	public void visit(ImpalaReduced impalaReduced) {
		throw new NotImplemented();
		// String[] s = impalaReduced.translate(Tags.REDUCED);
		// prependToScript(s[0]);
		// appendToScript(s[1]);
	}

	/**
	 * Translates an ORDER into corresponding SQL commands.
	 * 
	 * @param impalaOrder
	 *            ORDER in the ImpalaOp Tree
	 */
	@Override
	public void visit(ImpalaOrder impalaOrder) {
		SQLStatement s = impalaOrder.translate(Tags.ORDER, stack.pop());
		stack.push(s);

	}

	/**
	 * Translates a SLICE into corresponding Impala SQL commands.
	 * 
	 * @param impalaSlice
	 *            SLICE in the ImpalaOp Tree
	 */
	@Override
	public void visit(ImpalaSlice slice) {
		SQLStatement child = stack.pop();
		stack.push(slice.translate(child.getName(), child));
	}

}
