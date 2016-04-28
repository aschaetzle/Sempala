package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;

import de.uni_freiburg.informatik.dbis.sempala.translator.ImpalaOpVisitor;
import de.uni_freiburg.informatik.dbis.sempala.translator.Tags;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.Join;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.JoinType;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.JoinUtil;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.Schema;

/**
 *
 * @author Antony Neu
 */
public class ImpalaJoin extends ImpalaOp2 {

	// TODO: Proper implementation
	@SuppressWarnings("unused")
	private final OpJoin opJoin;

	public ImpalaJoin(OpJoin _opJoin, ImpalaOp _leftOp, ImpalaOp _rightOp,
			PrefixMapping _prefixes) {
		super(_leftOp, _rightOp, _prefixes);
		opJoin = _opJoin;
		resultName = Tags.JOIN;
	}

	@Override
	public SQLStatement translate(String _resultName, SQLStatement firstChild,
			SQLStatement secondChild) {

		resultName = _resultName;

		Map<String, String[]> newSchema = new HashMap<String, String[]>();
		newSchema.putAll(leftOp.getSchema());
		newSchema.putAll(rightOp.getSchema());
		resultSchema = Schema.shiftToParent(newSchema, this.resultName);


		SQLStatement join = null ;

		List<SQLStatement> rights = new ArrayList<SQLStatement>();
		rights.add(secondChild);

		List<String> onConditions =  JoinUtil.getOnConditions(Schema.shiftToParent(leftOp.getSchema(), leftOp.getResultName()), Schema.shiftToParent(rightOp.getSchema(), rightOp.getResultName()));


		join = new Join(this.getResultName(), firstChild, rights, onConditions, JoinType.natural);



		 return join;

	}



	@Override
	public void visit(ImpalaOpVisitor impalaOpVisitor) {
		impalaOpVisitor.visit(this);
	}

}
