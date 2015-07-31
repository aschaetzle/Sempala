package sparql2impala.op;

import java.util.HashMap;

import sparql2impala.sql.SQLStatement;
import sparql2impala.sql.Select;

import com.hp.hpl.jena.shared.PrefixMapping;

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
