package sparql2impala.op;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import sparql2impala.sql.SQLStatement;

import com.hp.hpl.jena.shared.PrefixMapping;

/**
 *
 * @author Antony Neu
 */
public abstract class ImpalaOpN extends ImpalaBase {

    protected List<ImpalaOp> elements = new ArrayList<ImpalaOp>();


    protected ImpalaOpN(PrefixMapping _prefixes) {
        prefixes = _prefixes;
        resultSchema = new HashMap<String, String[]>();
    }

    public ImpalaOp get(int index) {
        return elements.get(index);
    }

    public List<ImpalaOp> getElements() {
        return elements;
    }

    public void add(ImpalaOp op) {
        elements.add(op);
    }

    public void add(int index, ImpalaOp op) {
        elements.add(index, op);
    }

    public Iterator<ImpalaOp> iterator() {
        return elements.iterator();
    }

    public int size() {
        return elements.size();
    }

 
    
}
