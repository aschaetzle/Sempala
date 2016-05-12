package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;

import de.uni_freiburg.informatik.dbis.sempala.translator.Tags;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.Select;

/**
 * @author Manuel Schneider
 */
public class ImpalaBgpSingleTable extends ImpalaBGP {

	private StringBuilder fromClause;
	private List<String> onClauses;
	private List<String> whereClauses;
	Map<String, String> firstVariableOccurrence;

	public ImpalaBgpSingleTable(OpBGP opBGP, PrefixMapping prefixes) {
		super(opBGP, prefixes);
	}

	/*
	 * I := the set of all IRIs.
	 * L := the set of all RDF Literals
	 * B := the set of all blank nodes in RDF graphs
	 * T := I∪L∪B
	 * V := Variable set, infinite and disjoint from
	 *
	 * Definition: Triple Pattern
	 * (T∪V) x (I∪V) x (T∪V)
	 * (I∪L∪B∪V) x (I∪V) x (I∪L∪B∪V)
	 *
	 * Questions:
	 * - v1 p1 v1   allowed ? (Reflexivity
	 * - v1 v1 v1 ??
	 *
	 * Assumptions:
	 * - v1 p1 v1 is not allowed
	 * - All TP contain at least one variable
	 */

	class sortComp implements Comparator<Triple>{

		@Override
		public int compare(Triple o1, Triple o2) {


			return 0;
		}

	}


	@Override
	public SQLStatement translate(String resultName) {
		this.resultName = resultName;

		fromClause = new StringBuilder();
		onClauses = new ArrayList<>();
		whereClauses = new ArrayList<>();
		firstVariableOccurrence  = new HashMap<>();


		List<Triple> triples = opBGP.getPattern().getList();


		handleNode(triples.get(0).getSubject(),  Tags.SUBJECT_COLUMN_NAME, 0);
		handleNode(triples.get(0).getPredicate(),  Tags.PREDICATE_COLUMN_NAME, 0); // undefined if this is a variable
		handleNode(triples.get(0).getObject(), Tags.OBJECT_COLUMN_NAME, 0);
		fromClause.append(String.format("%s t0", Tags.IMPALA_SINGLETABLE_TABLENAME));

		for (int tripleIndex = 1; tripleIndex < triples.size(); ++tripleIndex) {
			onClauses = new ArrayList<>();
			handleNode(triples.get(tripleIndex).getSubject(),  Tags.SUBJECT_COLUMN_NAME, tripleIndex);
			handleNode(triples.get(tripleIndex).getPredicate(),  Tags.PREDICATE_COLUMN_NAME, tripleIndex); // undefined if this is a variable
			handleNode(triples.get(tripleIndex).getObject(), Tags.OBJECT_COLUMN_NAME, tripleIndex);
			fromClause.append(String.format("\nJOIN %s t%d", Tags.IMPALA_SINGLETABLE_TABLENAME, tripleIndex));
			if (!onClauses.isEmpty()){
				fromClause.append(String.format(" ON %s", onClauses.get(0)));
				for (int i = 1; i < onClauses.size(); ++i)
					fromClause.append(String.format(" AND %s", onClauses.get(i)));
			}
		}

		Select res = new Select(this.resultName + "_" + tableNumber++);

		for (Map.Entry<String, String> entry : firstVariableOccurrence.entrySet()) {
			String[] selectors = {entry.getValue()};
			res.addSelector(entry.getKey(), selectors);
		}

		for (String whereClause : whereClauses)
			res.addWhereConjunction(whereClause);

		res.setFrom(fromClause.toString());

		return res;
	}


	private void handleNode(Node node, String sqlNodeName, int tripleIndex){
		/*
		 * If this is a variable this is either a projection and/or a join node. If the
		 * variable occurs more than one time in the BGP this is a join node. By
		 * definition, if this is anything else than a variable, this is either an IRI,
		 * literal or a blank nodes. This does not matter to SQL simply add a where clause.
		 */
		if (node.isVariable()){
			if (firstVariableOccurrence.containsKey(node.getName()))
				onClauses.add(String.format("t%d.%s = %s", tripleIndex, sqlNodeName, firstVariableOccurrence.get(node.getName())));
			else
				firstVariableOccurrence.put(node.getName(), String.format("t%d.%s", tripleIndex, sqlNodeName));
		} else {
			whereClauses.add(String.format("t%d.%s = %s", tripleIndex, sqlNodeName, node));
		}
	}
}