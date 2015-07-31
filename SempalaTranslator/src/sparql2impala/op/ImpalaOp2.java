package sparql2impala.op;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import sparql2impala.sql.SQLStatement;
import sparql2impala.sql.Select;

import com.hp.hpl.jena.shared.PrefixMapping;

/**
 *
 * @author Antony Neu
 */
public abstract class ImpalaOp2 extends ImpalaBase {

    protected ImpalaOp leftOp, rightOp;

    
    protected ImpalaOp2(ImpalaOp _leftOp, ImpalaOp _rightOp, PrefixMapping _prefixes) {
        leftOp = _leftOp;
        rightOp = _rightOp;
        prefixes = _prefixes;
        resultSchema = new HashMap<String, String[]>();
    }
    
    public abstract SQLStatement translate(String name, SQLStatement left, SQLStatement right);

    public ImpalaOp getLeft() {
        return leftOp;
    }

    public ImpalaOp getRight() {
        return rightOp;
    }

/*
    protected ArrayList<String> getSharedVars() {
        Map<String, String[]> leftOpSchema = leftOp.getSchema();
        Map<String, String[]> rightOpSchema = rightOp.getSchema();
        ArrayList<String> sharedVars = new ArrayList<String>();

        for(String leftKey : leftOpSchema.keySet()) {
            if(rightOpSchema.containsKey(leftKey)) {
                sharedVars.add(leftKey);
            }
        }
        return sharedVars;
    }

    
    protected ArrayList<String> getOnlyLeftVars(){
        Map<String, String[]> leftOpSchema = leftOp.getSchema();
        Map<String, String[]> rightOpSchema = rightOp.getSchema();
        ArrayList<String> vars = new ArrayList<String>();

        for(String leftKey : leftOpSchema.keySet()) {
            if(!rightOpSchema.containsKey(leftKey)) {
            	vars.add(leftKey);
            }
        }
        return vars;
    }

    
    
    protected ArrayList<String> getOnlyRightVars(){
        Map<String, String[]> leftOpSchema = leftOp.getSchema();
        Map<String, String[]> rightOpSchema = rightOp.getSchema();
        ArrayList<String> vars = new ArrayList<String>();

        for(String leftKey : leftOpSchema.keySet()) {
            if(!rightOpSchema.containsKey(leftKey)) {
            	vars.add(leftKey);
            }
        }
        return vars;
    }

 */

}
