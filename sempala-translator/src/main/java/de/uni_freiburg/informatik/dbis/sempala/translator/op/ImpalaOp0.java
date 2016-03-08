package de.uni_freiburg.informatik.dbis.sempala.translator.op;

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
