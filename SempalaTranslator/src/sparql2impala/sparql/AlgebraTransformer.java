package sparql2impala.sparql;

import java.util.Stack;

import sparql2impala.op.ImpalaBGP;
import sparql2impala.op.ImpalaDistinct;
import sparql2impala.op.ImpalaFilter;
import sparql2impala.op.ImpalaJoin;
import sparql2impala.op.ImpalaLeftJoin;
import sparql2impala.op.ImpalaOp;
import sparql2impala.op.ImpalaOrder;
import sparql2impala.op.ImpalaProject;
import sparql2impala.op.ImpalaReduced;
import sparql2impala.op.ImpalaSequence;
import sparql2impala.op.ImpalaSlice;
import sparql2impala.op.ImpalaUnion;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpConditional;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;

/**
 *
 * @author Antony Neu
 */
public class AlgebraTransformer extends OpVisitorBase {

    private final Stack<ImpalaOp> stack;
    private final PrefixMapping prefixes;


    public AlgebraTransformer(PrefixMapping _prefixes) {
        stack = new Stack<ImpalaOp>();
        prefixes = _prefixes;
    }

    public ImpalaOp transform(Op op) {
        AlgebraWalker.walkBottomUp(this, op);
        return stack.pop();
    }


    
    @Override
    public void visit(OpBGP opBGP) {
        stack.push(new ImpalaBGP(opBGP, prefixes));
    }

    @Override
    public void visit(OpFilter opFilter) {
        ImpalaOp subOp = stack.pop();
        stack.push(new ImpalaFilter(opFilter, subOp, prefixes));
    }

    @Override
    public void visit(OpJoin opJoin) {
        ImpalaOp rightOp = stack.pop();
        ImpalaOp leftOp = stack.pop();
        stack.push(new ImpalaJoin(opJoin, leftOp, rightOp, prefixes));
    }

    @Override
    public void visit(OpSequence opSequence) {
        ImpalaSequence ImpalaSequence = new ImpalaSequence(opSequence, prefixes);
        for(int i=0; i<opSequence.size(); i++) {
            ImpalaSequence.add(0,stack.pop());
        }
        stack.push(ImpalaSequence);
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        ImpalaOp rightOp = stack.pop();
        ImpalaOp leftOp = stack.pop();
        stack.push(new ImpalaLeftJoin(opLeftJoin, leftOp, rightOp, prefixes));
    }

    /*
    @Override
    public void visit(OpConditional opConditional) {
        ImpalaOp rightOp = stack.pop();
        ImpalaOp leftOp = stack.pop();
        stack.push(new ImpalaConditional(opConditional, leftOp, rightOp, prefixes));
    } */

    @Override
     public void visit(OpUnion opUnion) {
        ImpalaOp rightOp = stack.pop();
        ImpalaOp leftOp = stack.pop();
        stack.push(new ImpalaUnion(opUnion, leftOp, rightOp, prefixes));
    }

    @Override
    public void visit(OpProject opProject) {
        ImpalaOp subOp = stack.pop();
        stack.push(new ImpalaProject(opProject, subOp, prefixes));
    }

    @Override
    public void visit(OpDistinct opDistinct) {
        ImpalaOp subOp = stack.pop();
        stack.push(new ImpalaDistinct(opDistinct, subOp, prefixes));
    }

    @Override
    public void visit(OpOrder opOrder) {
        ImpalaOp subOp = stack.pop();
        stack.push(new ImpalaOrder(opOrder, subOp, prefixes));
    }

    @Override
    public void visit(OpSlice opSlice) {
        ImpalaOp subOp = stack.pop();
        // change order of tree nodes
        if(subOp instanceof ImpalaProject){
        	ImpalaSlice slice = new ImpalaSlice(opSlice, ((ImpalaProject) subOp).getSubOp(), prefixes);
        	ImpalaProject project = new ImpalaProject(((ImpalaProject) subOp).getOpProject(), (ImpalaOp)slice, prefixes);
        	stack.push(project);
        } else {
        stack.push(new ImpalaSlice(opSlice, subOp, prefixes));
        }
    }

    @Override
    public void visit(OpReduced opReduced) {
        ImpalaOp subOp = stack.pop();
        stack.push(new ImpalaReduced(opReduced, subOp, prefixes));
    }

}
