package sparql2impala.op;

import sparql2impala.ImpalaOpVisitor;
import sparql2impala.Tags;
import sparql2impala.sql.SQLStatement;
import sparql2impala.sql.Schema;
import sparql2impala.sql.Select;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;

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
		boolean first = true;
		for (Var var : this.opProject.getVars()) {
			projection.addSelector(var.getName(), resultSchema.get(var.getName()));
			first = false;
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
