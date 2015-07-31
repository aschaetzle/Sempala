package sparql2impala.op;

import java.util.ArrayList;
import java.util.HashMap;

import com.hp.hpl.jena.shared.PrefixMapping;

/**
 *
 * @author Antony Neu
 */
public abstract class ImpalaOp0 extends ImpalaBase {

    protected ImpalaOp0(PrefixMapping _prefixes) {
        prefixes = _prefixes;
        resultSchema = new HashMap<String,String[]>();
    }

}
