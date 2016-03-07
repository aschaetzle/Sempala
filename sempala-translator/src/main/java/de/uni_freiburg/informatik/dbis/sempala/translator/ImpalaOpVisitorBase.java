package de.uni_freiburg.informatik.dbis.sempala.translator;


import org.apache.log4j.Logger;

import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaBGP;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaDistinct;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaFilter;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaJoin;
import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaLeftJoin;
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
public class ImpalaOpVisitorBase implements ImpalaOpVisitor {
    
    // Define a static logger variable so that it references the corresponding Logger instance
    protected static Logger logger = Logger.getLogger(ImpalaOpVisitor.class);

    // OPERATORS
    @Override
    public void visit(ImpalaBGP impalaBGP) {
        logger.error("BGP not supported yet!");
        throw new UnsupportedOperationException("BGP not supported yet!");
    }

    @Override
    public void visit(ImpalaFilter impalaFilter) {
        logger.error("FILTER not supported yet!");
        throw new UnsupportedOperationException("FILTER not supported yet!");
    }

    @Override
    public void visit(ImpalaJoin impalaJoin) {
        logger.error("JOIN not supported yet!");
        throw new UnsupportedOperationException("JOIN not supported yet!");
    }

    @Override
    public void visit(ImpalaSequence impalaSequence) {
        logger.error("SEQUENCE not supported yet!");
        throw new UnsupportedOperationException("SEQUENCE not supported yet!");
    }

    @Override
    public void visit(ImpalaLeftJoin impalaLeftJoin) {
        logger.error("LEFTJOIN not supported yet!");
        throw new UnsupportedOperationException("LEFTJOIN not supported yet!");
    }

    /* @Override
   public void visit(ImpalaConditional impalaConditional) {
        logger.error("CONDITIONAL not supported yet!");
        throw new UnsupportedOperationException("CONDITIONAL not supported yet!");
    }*/

    @Override
    public void visit(ImpalaUnion impalaUnion) {
        logger.error("UNION not supported yet!");
        throw new UnsupportedOperationException("UNION not supported yet!");
    }

    // SOLUTION MODIFIERS
    @Override
    public void visit(ImpalaProject impalaProject) {
        logger.error("PROJECT not supported yet!");
        throw new UnsupportedOperationException("PROJECT not supported yet!");
    }

    @Override
    public void visit(ImpalaDistinct impalaDistinct) {
        logger.error("DISTINCT not supported yet!");
        throw new UnsupportedOperationException("DISTINCT not supported yet!");
    }

    @Override
    public void visit(ImpalaReduced impalaReduced) {
        logger.error("REDUCED not supported yet!");
        throw new UnsupportedOperationException("REDUCED not supported yet!");
    }

    @Override
    public void visit(ImpalaOrder impalaOrder) {
        logger.error("ORDER not supported yet!");
        throw new UnsupportedOperationException("ORDER not supported yet!");
    }

    @Override
    public void visit(ImpalaSlice impalaSlice) {
        logger.error("SLICE not supported yet!");
        throw new UnsupportedOperationException("SLICE not supported yet!");
    }
    
    

}
