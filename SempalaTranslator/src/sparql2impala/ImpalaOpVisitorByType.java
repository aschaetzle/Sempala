package sparql2impala;


import org.apache.log4j.Logger;

import sparql2impala.op.ImpalaBGP;
import sparql2impala.op.ImpalaDistinct;
import sparql2impala.op.ImpalaFilter;
import sparql2impala.op.ImpalaJoin;
import sparql2impala.op.ImpalaLeftJoin;
import sparql2impala.op.ImpalaOp;
import sparql2impala.op.ImpalaOp1;
import sparql2impala.op.ImpalaOp2;
import sparql2impala.op.ImpalaOpN;
import sparql2impala.op.ImpalaOrder;
import sparql2impala.op.ImpalaProject;
import sparql2impala.op.ImpalaReduced;
import sparql2impala.op.ImpalaSequence;
import sparql2impala.op.ImpalaSlice;
import sparql2impala.op.ImpalaUnion;

/**
 *
 * @author Antony Neu
 */
public abstract class ImpalaOpVisitorByType implements ImpalaOpVisitor {
    
    // Define a static logger variable so that it references the corresponding Logger instance
    protected static Logger logger = Logger.getLogger(ImpalaOpVisitor.class);
    

    /**
     * Operators with no sub operators
     * @param op 
     */
    protected abstract void visit0(ImpalaOp op);

    /**
     * Operators with 1 sub operator
     * @param op 
     */ 
    protected abstract void visit1(ImpalaOp1 op);

    /**
     * Operators with 2 sub operators
     * @param op 
     */
    protected abstract void visit2(ImpalaOp2 op);

    /**
     * Operators with N sub operators
     * @param op 
     */
    protected abstract void visitN(ImpalaOpN op);
    

    // Declare basic visit methods as final such that derived classes cannot override it
    // OPERATORS
    @Override
    public final void visit(ImpalaBGP ImpalaBGP) {
        visit0(ImpalaBGP);
    }
    
    @Override
    public final void visit(ImpalaFilter ImpalaFilter) {
        visit1(ImpalaFilter);
    }

    @Override
    public final void visit(ImpalaJoin ImpalaJoin) {
        visit2(ImpalaJoin);
    }

    @Override
    public final void visit(ImpalaSequence ImpalaSequence) {
        visitN(ImpalaSequence);
    }

    @Override
    public final void visit(ImpalaLeftJoin ImpalaLeftJoin) {
        visit2(ImpalaLeftJoin);
    }

    
    /*
    @Override
    public final void visit(ImpalaConditional ImpalaConditional) {
        visit2(ImpalaConditional);
    } */

    @Override
    public final void visit(ImpalaUnion ImpalaUnion) {
        visit2(ImpalaUnion);
    }

    // SOLUTION MODIFIERS
    @Override
    public final void visit(ImpalaProject ImpalaProject) {
        visit1(ImpalaProject);
    }

    @Override
    public final void visit(ImpalaDistinct ImpalaDistinct) {
        visit1(ImpalaDistinct);
    }

    @Override
    public final void visit(ImpalaReduced ImpalaReduced) {
        visit1(ImpalaReduced);
    }

    @Override
    public final void visit(ImpalaOrder ImpalaOrder) {
        visit1(ImpalaOrder);
    }

    @Override
    public final void visit(ImpalaSlice ImpalaSlice) {
        visit1(ImpalaSlice);
    }

}
