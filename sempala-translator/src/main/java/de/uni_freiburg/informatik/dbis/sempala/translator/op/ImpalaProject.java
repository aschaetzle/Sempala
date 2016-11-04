package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;

import de.uni_freiburg.informatik.dbis.sempala.translator.ImpalaOpVisitor;
import de.uni_freiburg.informatik.dbis.sempala.translator.Tags;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.Schema;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.Select;

/**
 *
 * @author Antony Neu
 */
public class ImpalaProject extends ImpalaOp1 {

	private final OpProject opProject;

	public ImpalaProject(OpProject _opProject, ImpalaOp _subOp,
			PrefixMapping _prefixes) {
		super(_subOp, _prefixes);
		opProject = _opProject;
		resultName = Tags.PROJECT;
	}


	@Override
	public SQLStatement translate(String _resultName, SQLStatement child) {
		// update schema
		resultSchema = Schema.shiftToParent(subOp.getSchema(), subOp.getResultName());
		resultName = _resultName;

		// translates to select object
		Select projection = new Select(this.getResultName());

		// Add selectors (subset of result schema)
		for (Var var : this.opProject.getVars()) {
			projection.addSelector(var.getName(), resultSchema.get(var.getName()));
		}

		// set from
		projection.setFrom(child.toNamedString());

		return projection;
	}

	public OpProject getOpProject(){
		return this.opProject;
	}

	@Override
	public void visit(ImpalaOpVisitor impalaOpVisitor) {
		impalaOpVisitor.visit(this);
	}



}
