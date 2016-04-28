package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;

import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.Select;

/**
 *
 * @author Antony Neu
 */
public class ImpalaBgpSingleTable extends ImpalaBGP {

	public ImpalaBgpSingleTable(OpBGP opBGP, PrefixMapping prefixes) {
		super(opBGP, prefixes);
	}

	@Override
	public SQLStatement translate(String _resultName) {
		return new Select("TÄÄÄST");
	}
}