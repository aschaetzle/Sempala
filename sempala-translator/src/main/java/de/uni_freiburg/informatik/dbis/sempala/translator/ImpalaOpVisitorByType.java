package de.uni_freiburg.informatik.dbis.sempala.translator;


import org.apache.log4j.Logger;

import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaBGP;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaDistinct;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaFilter;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaJoin;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaLeftJoin;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaOp;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaOp1;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaOp2;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaOpN;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaOrder;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaProject;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaReduced;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaSequence;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaSlice;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaUnion;

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
