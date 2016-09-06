package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.util.FmtUtils;

import de.uni_freiburg.informatik.dbis.sempala.translator.Tags;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.Join;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.JoinType;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.Select;

/**
 * @author Manuel Schneider
 */
public class ImpalaBgpSingleTable extends ImpalaBGP {

	private Map<Triple, List<Node>> varIndex;
	private Map<Node, List<Triple>> invertedVarIndex;

	public ImpalaBgpSingleTable(OpBGP opBGP, PrefixMapping prefixes) {
		super(opBGP, prefixes);
	}

	/*
	 * I := the set of all IRIs.
	 * B := the set of all blank nodes in RDF graphs
	 * L := the set of all RDF Literals
	 * T := I∪L∪B
	 * V := Variable set, infinite and disjoint from
	 *
	 * Definition: Triple Pattern
	 * (T∪V) x (I∪V) x (T∪V)
	 * (I∪L∪B∪V) x (I∪V) x (I∪L∪B∪V)
	 *
	 */

	@Override
	public SQLStatement translate(String resultName) {
		this.resultName = resultName;
		List<Triple> unhandledTriples = opBGP.getPattern().getList();

		// empty PrefixMapping when prefixes should be expanded
		if (expandPrefixes)
			prefixes = PrefixMapping.Factory.create();

		// Build variable index
		varIndex = new HashMap<>();
		for ( Triple triple : unhandledTriples )
			for (Node node : Arrays.asList(triple.getSubject(), triple.getPredicate(), triple.getObject()) )
				if ( node.isVariable() ) {
					if ( !varIndex.containsKey(triple) )
						varIndex.put(triple, new ArrayList<Node>());
					varIndex.get(triple).add(node);
				}

		// Build inverted variable index
		invertedVarIndex = new HashMap<>();
		for ( Entry<Triple, List<Node>> entry : varIndex.entrySet() )
			for ( Node varNode : entry.getValue())  {
				if ( !invertedVarIndex.containsKey(varNode) )
					invertedVarIndex.put(varNode, new ArrayList<Triple>());
				invertedVarIndex.get(varNode).add(entry.getKey());
			}


		/*
		 * Compute join order and partitions using neighborhood algorithm
		 */

		List<List<Triple>> partitions = new ArrayList<>();

		while (!unhandledTriples.isEmpty()){

			Queue<Triple> neighborhood = new LinkedList<>();
			List<Triple> partition = new ArrayList<>();

			// Begin with a random one
			neighborhood.add(unhandledTriples.remove(unhandledTriples.size()-1));

			while (!neighborhood.isEmpty()){

				// Get next element in neighborhood queue
				Triple triple = neighborhood.remove();

				// Mark it as handled
				unhandledTriples.remove(triple);

				// Add it to this partition
				partition.add(triple);

				// Add every unhandled triple that shares a variable with
				// triple to the neighborhood. Ignore duplicates.
				for ( Node varNode : varIndex.get(triple) )
					for ( Triple neighbor : invertedVarIndex.get(varNode) )
						if ( unhandledTriples.contains(neighbor) && !neighborhood.contains(neighbor))
							neighborhood.add(neighbor);
			}

			partitions.add(partition);
		}


		/*
		 * Join the select statements in the partitions (inner joins)
		 */

		List<SQLStatement> joinedPartitions = new ArrayList<>();

		for ( int partitionIndex = 0; partitionIndex < partitions.size(); ++partitionIndex ) {

			// Build the components of the join
			List<SQLStatement> subqueries = new ArrayList<>();
			List<String> onClauses = new ArrayList<>();
			Map<Node, Integer> initialVarIndizes = new HashMap<>();

			// Special iteration beginning step, check comments of the iteration below
			subqueries.add(buildSelectStmt(partitions.get(partitionIndex).get(0), "Sub0"));
			for ( Node variable : varIndex.get(partitions.get(partitionIndex).get(0)) )
				initialVarIndizes.put(variable, 0);
			onClauses.add("");

			for ( int index = 1; index < partitions.get(partitionIndex).size(); ++index ) {

				// Build the sub query
				subqueries.add(buildSelectStmt(partitions.get(partitionIndex).get(index), "Sub"+index));

				/*
				 *  Build the on clause.
				 *  If this is the first time a variable occurs remember the
				 *  index, else join with the first occurrence. This is a bit
				 *  of a mimimal LOC hack. Add all clauses to onClauses and
				 *  finally pop them and append them to the current onClause.
				 */
				for ( Node variable : varIndex.get(partitions.get(partitionIndex).get(index)) ) {
					if ( initialVarIndizes.containsKey(variable) ) {
						onClauses.add(String.format("Sub%2$d.%1$s = Sub%3$d.%1$s", variable.getName(), index, initialVarIndizes.get(variable)));
					} else {
						initialVarIndizes.put(variable, index);
					}
				}
				while ( subqueries.size() < onClauses.size() )
					onClauses.get(index).concat(" AND " + onClauses.remove(onClauses.size()-1));
			}

			/*
			 * If there are multiple triples in this partition. join them. If
			 * not, rename the subquery to fit the higher level partition names.
			 */
			if ( subqueries.size() > 1 ) {
				joinedPartitions.add(new Join("P"+partitionIndex, subqueries.get(0),
						subqueries.subList(1, subqueries.size()), onClauses.subList(1, onClauses.size()), JoinType.INNER));
			} else {
				((Select)subqueries.get(0)).setName("P"+partitionIndex);
				joinedPartitions.add(subqueries.get(0));
			}
		}


		/*
		 * Join the partitions (cross joins)
		 */

		SQLStatement result;

		// If there are multiple partitions cross join them
		if ( partitions.size() > 1 ) {
			SQLStatement left = joinedPartitions.get(0);
			List<SQLStatement> rights = new ArrayList<>();
			rights.addAll(joinedPartitions.subList(1, joinedPartitions.size()));
			List<String> onClauses = new ArrayList<>(Collections.nCopies(rights.size(), ""));
			result = new Join("res", left, rights, onClauses, JoinType.CROSS);
		}
		else
			result = joinedPartitions.get(0);

		// Interface Antony's code
		this.resultName = result.getName();
		for ( Node var : invertedVarIndex.keySet() )
			this.resultSchema.put(var.getName(), new String[0]);

		return result;
	}



	/**
	 * Builds a select statement using the inverted variable index.
	 * @param triple
	 * @param referenceName
	 * @return
	 */
	private Select buildSelectStmt(Triple triple, String referenceName){

		Select stmt = new Select(referenceName);
		stmt.setFrom(Tags.IMPALA_SINGLETABLE_TABLENAME);

		/*
		 * If the subject is a variable check if there are any relations and
		 * use them to increase selectivity. If the subject is not a variable
		 * add a regular WHERE condition containing the IRI or literal.
		 */
		if ( triple.getSubject().isVariable() ){

			stmt.addSelector(triple.getSubject().getName(), new String[]{Tags.SUBJECT_COLUMN_NAME} );

			for ( Triple neighbour : invertedVarIndex.get(triple.getSubject()) ) {

				// Skip itself
				if ( neighbour == triple )
					continue;

				// Predicate is mandatory for the SS,SO,OS relations
				if (neighbour.getPredicate().isVariable())
					continue;

				// If the neighbour.s equals triple.s add SS relation. Implies neighbour.getSubject().isVariable().
				if (neighbour.getSubject().equals(triple.getSubject()) )
					stmt.addWhereConjunction(String.format("ss_%s",
							toImpalaColumnName(FmtUtils.stringForNode(neighbour.getPredicate(), prefixes))));

				// Analogous SO relations
				if (neighbour.getObject().equals(triple.getSubject()) )
					stmt.addWhereConjunction(String.format("so_%s",
							toImpalaColumnName(FmtUtils.stringForNode(neighbour.getPredicate(), prefixes))));
			}
		}
		else
			stmt.addWhereConjunction(String.format("%s = '%s'", Tags.SUBJECT_COLUMN_NAME,
					FmtUtils.stringForNode(triple.getSubject(), this.prefixes)));

		/*
		 * If the object is a variable check if there are any relations and
		 * use them to increase selectivity. If the subject is not a variable
		 * add a regular WHERE condition containing the IRI or literal.
		 */
		if ( triple.getPredicate().isVariable() ){
			stmt.addSelector(triple.getPredicate().getName(), new String[]{Tags.PREDICATE_COLUMN_NAME} );
		}
		else
			stmt.addWhereConjunction(String.format("%s = '%s'", Tags.PREDICATE_COLUMN_NAME,
					FmtUtils.stringForNode(triple.getPredicate(), this.prefixes)));

		/*
		 * If the object is a variable check if there are any relations and
		 * use them to increase selectivity. If the subject is not a variable
		 * add a regular WHERE condition containing the IRI or literal.
		 */
		if ( triple.getObject().isVariable() ){

			stmt.addSelector(triple.getObject().getName(), new String[]{Tags.OBJECT_COLUMN_NAME} );

			for ( Triple neighbour : invertedVarIndex.get(triple.getObject()) ) {

				// Predicate is mandatory for the SS,SO,OS relations
				if (neighbour.getPredicate().isVariable())
					continue;

				// If the neighbour.s equals triple.o add OS relation. Implies neighbour.getSubject().isVariable().
				if (neighbour.getSubject().equals(triple.getObject()) )
					stmt.addWhereConjunction(String.format("os_%s",
							toImpalaColumnName(FmtUtils.stringForNode(neighbour.getPredicate(), prefixes))));
			}
		}
		else
			stmt.addWhereConjunction(String.format("%s = '%s'", Tags.OBJECT_COLUMN_NAME,
					FmtUtils.stringForNode(triple.getObject(), this.prefixes)));

		return stmt;
	}



	/**
	 * Makes the string conform to the requirements for impala column names.
	 * I.e. remove braces, replace non word characters, trim spaces.
	 * @param The string to make impala conform
	 * @return The impala conform string
	 */
	private static String toImpalaColumnName(String s) {
		// Space is a nonword character so trim before replacing chars.
		return s.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
	}

}