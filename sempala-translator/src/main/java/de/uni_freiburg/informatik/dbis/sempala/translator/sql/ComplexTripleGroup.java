package de.uni_freiburg.informatik.dbis.sempala.translator.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.util.FmtUtils;

import de.uni_freiburg.informatik.dbis.sempala.translator.ComplexPropertyTableColumns;
import de.uni_freiburg.informatik.dbis.sempala.translator.Tags;

//TODO change comments
/**
 * Group of triples within a BGP.
 *
 * @author neua
 *
 */
public class ComplexTripleGroup {

	private List<Triple> triples = new ArrayList<Triple>();

	private String name;


	public String getName() {
		return name;
	}

	/**
	 * If the same predicate is selected twice, then a cross join is needed.
	 */ 
	ArrayList<Triple> crossjoin = new ArrayList<Triple>();
	private Map<String, String[]> crossJoinMapping = new HashMap<String, String[]>();

	// choose triplestore as predicate unbound
	private boolean selectFromTripleStore = false;

	PrefixMapping prefixMapping;

	// for all variables in the query we add their name and their type
	// for example if we have ?sVariable p1 ?oVariable
	// in the mapping we will add <<sVariable, s>, <oVariable, o>>
	private Map<String, String[]> mapping = new HashMap<String, String[]>();

	public ComplexTripleGroup(String tablename, PrefixMapping mapping,
			boolean selectFromTripleStore) {
		this.name = tablename;
		this.prefixMapping = mapping;
		this.selectFromTripleStore = selectFromTripleStore;
	}

	public void add(Triple triple) {
		triples.add(triple);
		mapping.putAll(getMappingVarsOfTriple(triple));

	}

	private HashMap<String, String[]> getMappingVarsOfTriple(Triple t) {
		HashMap<String, String[]> result = new HashMap<String, String[]>();
		Node subject = t.getSubject();
		Node predicate = t.getPredicate();
		Node object = t.getObject();
		if (subject.isVariable()){
			result.put(subject.getName(),
					new String[] { Tags.SUBJECT_COLUMN_NAME });
		}
		if (predicate.isVariable()) {
			selectFromTripleStore = true;
			result.put(predicate.getName(),
					new String[] { Tags.PREDICATE_COLUMN_NAME });
		}
		if (object.isVariable()) {
			if (selectFromTripleStore) {
				result.put(object.getName(),
						new String[] { Tags.OBJECT_COLUMN_NAME });
			} else {
				String objectString = object.getName();
				String predicateString = FmtUtils
						.stringForNode(predicate, prefixMapping);
				
				// TODO ask why are we doing this here. Please add comment
				if(predicateString.startsWith("<") && predicateString.endsWith(">"))
					predicateString = predicateString.substring(1, predicateString.length() -1);
				
				result.put(objectString, new String[] { SpecialCharFilter
						.filter(predicateString) });
			}
		}
		return result;
	}

	public SQLStatement translate() {
		ComplexSelect select = new ComplexSelect(this.name);
		select.setComplexColumns(new HashMap<String,Boolean>(ComplexPropertyTableColumns.getColumns()));
		
		// if one of the properties is a complex one, set the select accordingly
		for (int i = 0; i < triples.size(); i++) {
			String predicateString = FmtUtils.stringForNode(triples.get(i).getPredicate(), this.prefixMapping);
			if(select.is_complex_column.get(SpecialCharFilter.filter(predicateString))){
					select.setComplexVariables();
					break;
			}
		
		}
		
		ArrayList<String> vars = new ArrayList<String>();
		ArrayList<String> whereConditions = new ArrayList<String>();
		boolean first = true;
		for (int i = 0; i < triples.size(); i++) {
			Triple triple = triples.get(i);
			Node subject = triple.getSubject();
			Node predicate = triple.getPredicate();
			Node object = triple.getObject();

			if (first) {
				first = false;
				// only check subject once per group
				// TODO WHat does it mean that the node is blank
				if (subject.isURI() || subject.isBlank()) {
					// subject is bound -> add to Filter
					String subjectString = FmtUtils
							.stringForNode(subject, this.prefixMapping);
					
					whereConditions.add(Tags.SUBJECT_COLUMN_NAME + " = '"
							+ subjectString + "'");
				} else {

					vars.add(subject.getName());
					whereConditions.add(Tags.SUBJECT_COLUMN_NAME
							+ " IS NOT NULL ");
				}
			}
			
			if (predicate.isURI()) {
				
				// TODO why we do that as we already when we add a new triple check for triples with same predicate
				// cross join needed?
				int index = searchTripleSamePredicate(i);
				while (index != -1) {
					crossjoin.add(triples.get(index));
					triples.remove(index);
					index = searchTripleSamePredicate(i);
				}
				
				// predicate is bound -> add to Filter
				String predicateString = FmtUtils
						.stringForNode(predicate, this.prefixMapping);
				
				//TODO add comments why we have this
				if(predicateString.startsWith("<") && predicateString.endsWith(">"))
					predicateString = predicateString.substring(1, predicateString.length() -1);
				
				whereConditions.add(SpecialCharFilter.filter(predicateString)
						+ " IS NOT NULL");

			} else {
				String predicateString = predicate.getName();
				
				// TODO what is this? add comment
				if(predicateString.endsWith(">"))
					predicateString = predicateString.substring(0, predicateString.length() -1);
				vars.add(predicateString);
			}
			
			if (object.isURI() || object.isLiteral() || object.isBlank()) {
				String string = FmtUtils.stringForNode(object,
						this.prefixMapping);
				
				if (object.isLiteral()) {
					string = "" + object.getLiteral().getValue();
				}
				String condition = "";
				if (selectFromTripleStore) {
					condition = Tags.OBJECT_COLUMN_NAME + " = '"
							+ string + "'";
				} else {
					String predicateString = FmtUtils
							.stringForNode(predicate, this.prefixMapping);
					
					//TODO what is this? add comment
					if(predicateString.endsWith(">"))
						predicateString = predicateString.substring(1, predicateString.length() -1);
					
					condition = SpecialCharFilter.filter(predicateString)
							+ " = '" + string + "'";
				}
				whereConditions.add(condition);
			} else {
				vars.add(object.getName());
			}
		}

		for (String var : vars) {
			String[] mapsTo = mapping.get(var);
			if (mapsTo.length > 1) {
				select.addSelector(var, new String[] { mapsTo[1] });
			} else {
				select.addSelector(var, new String[] { mapsTo[0] });
			}
		}

		// FROM
		if (selectFromTripleStore) {
			select.setFrom(Tags.IMPALA_TABLENAME_TRIPLESTORE);
		} else {
			select.setFrom(Tags.COMPLEX_PROPERTYTABLE_TABLENAME);
		}
		// WHERE
		for (String where : whereConditions) {
			select.addWhereConjunction(where);
		}


		return select;
	}

	public int getSharedVars(ComplexTripleGroup other) {
		return JoinUtil.getSharedVars(this.mapping, other.getMappings()).size();
	}

	/**
	 * Join with another TripleGroup.
	 */

	public void join(ComplexTripleGroup other) {
		for (String entry : other.mapping.keySet()) {
			if (!mapping.containsKey(entry)) {
				mapping.put(entry, other.mapping.get(entry));
			}
		}

	}

	public int searchTripleSamePredicate(int index) {
		for (int i = 0; i < this.triples.size(); i++) {
			if (i != index
					&& triples.get(index).getPredicate()
					.equals(triples.get(i).getPredicate())) {
				return i;
			}
		}

		return -1;
	}

	public Map<String, String[]> getMappings() {
		Map<String, String[]> temp = new HashMap<String, String[]>();
		// Merge both mappings
		temp.putAll(this.mapping);
		temp.putAll(crossJoinMapping);
		return temp;
	}

	// Changes second part of entry leaving everything else in tact.
	public void shiftOrigin(String parent) {
		this.mapping = Schema.shiftOrigin(this.mapping, parent);
	}

	public void setMapping(Map<String, String[]> mapping) {
		this.mapping = mapping;
	}
}
