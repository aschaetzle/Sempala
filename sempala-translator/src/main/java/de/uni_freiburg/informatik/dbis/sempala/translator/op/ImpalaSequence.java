package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import java.util.ArrayList;
import java.util.Map;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;

import de.uni_freiburg.informatik.dbis.sempala.translator.ImpalaOpVisitor;
import de.uni_freiburg.informatik.dbis.sempala.translator.Tags;

/**
 *
 * @author Antony Neu
 */
public class ImpalaSequence extends ImpalaOpN {

	// TODO: Proper implementation
	@SuppressWarnings("unused")
	private final OpSequence opSequence;
	@SuppressWarnings("unused")
	private ArrayList<String> intermediateSchema;


	public ImpalaSequence(OpSequence _opSequence, PrefixMapping _prefixes) {
		super(_prefixes);
		opSequence = _opSequence;
		resultName = Tags.SEQUENCE;
	}



//    private String generateSequence() {
//        String sequence = "";
//        intermediateSchema.addAll(get(0).getSchema());
//        for(int i=1; i<size(); i++) {
//            // JOIN
//            sequence += "tmp" + i + " = ";
//            ImpalaOp joinOp = get(i);
//            ArrayList<String> sharedVars = getSharedVars(intermediateSchema, joinOp.getSchema());
//            if (checkForNullJoin(sharedVars)) {
//                throw new UnsupportedOperationException("Query leads to a Join on Null values in SEQUENCE clause!");
//            }
//
//            if (sharedVars.isEmpty()) {
//                sequence += "CROSS " + ((i==1)? get(0).getResultName() : "j"+(i-1)) + ", " + joinOp.getResultName();
//            }
//            else {
//                sequence += "JOIN ";
//                String joinArg = toArgumentList(sharedVars);
//                sequence += ((i==1)? get(0).getResultName() : "j"+(i-1)) + " BY " + joinArg + ", ";
//                sequence += joinOp.getResultName() + " BY " + joinArg;
//            }
//            sequence += " PARALLEL $reducerNum ;\n";
//
//            // FOREACH
//            sequence += ((i==size()-1)? resultName : "j"+i) + " = FOREACH tmp" + i + " GENERATE ";
//            String field = intermediateSchema.get(0);
//            // intermediateSchema completely
//            sequence += "$0" + " AS " + field;
//            for (int j=1; j<intermediateSchema.size(); j++) {
//                field = intermediateSchema.get(j);
//                sequence += ", $" + j + " AS " + field;
//            }
//            // schema of joinOp without shared variables
//            ArrayList<String> joinOpSchema = joinOp.getSchema();
//            int schemaSize = intermediateSchema.size();
//            for (int j=0; j<joinOpSchema.size(); j++) {
//                field = joinOpSchema.get(j);
//                if (!intermediateSchema.contains(field)) {
//                    sequence += ", $" + (schemaSize+j) + " AS " + field;
//                    intermediateSchema.add(field);
//                }
//            }
//            sequence += " ;\n";
//        }
//        return sequence;
//    }



	@Override
	public void visit(ImpalaOpVisitor impalaOpVisitor) {
		impalaOpVisitor.visit(this);
	}

	public void setSchema(Map<String, String[]> schema){
		this.resultSchema = schema;

	}

}
