package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import java.util.HashMap;

import com.hp.hpl.jena.shared.PrefixMapping;

import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;

/**
 *
 * @author Antony Neu
 */
public abstract class ImpalaOp1 extends ImpalaBase {

    protected ImpalaOp subOp;


    protected ImpalaOp1(ImpalaOp _subOp, PrefixMapping _prefixes) {
        subOp = _subOp;
        prefixes = _prefixes;
        resultSchema = new HashMap<String, String[]>();
    }

    public ImpalaOp getSubOp() {
        return subOp;
    }

    public abstract SQLStatement translate(String name, SQLStatement child);
    
}
