package de.uni_freiburg.informatik.dbis.sempala.translator;


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
public interface ImpalaOpVisitor {
    
    // Operators
    public void visit(ImpalaBGP impalaBGP);
    public void visit(ImpalaFilter impalaFilter);
    public void visit(ImpalaJoin impalaJoin);
    public void visit(ImpalaSequence impalaSequence);
    public void visit(ImpalaLeftJoin impalaLeftJoin);
    //public void visit(ImpalaConditional impalaConditional);
    public void visit(ImpalaUnion impalaUnion);

    // Solution Modifier
    public void visit(ImpalaProject impalaProject);
    public void visit(ImpalaDistinct impalaDistinct);
    public void visit(ImpalaReduced impalaReduced);
    public void visit(ImpalaOrder impalaOrder);
    public void visit(ImpalaSlice impalaSlice);

}
