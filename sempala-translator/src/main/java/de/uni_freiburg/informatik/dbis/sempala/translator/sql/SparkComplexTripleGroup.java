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

/**
 * A class representing group of triples with the same subject. From them
 * conditions in the where clause of a select are constructed. If a complex
 * property is part of the where clause "array_contains" function is used.
 * 
 * @author Polina Koleva
 *
 */
public class SparkComplexTripleGroup {

	private List<Triple> triples = new ArrayList<Triple>();

	private String name;

	public String getName() {
		return name;
	}

	// choose triplestore as predicate unbound
	private boolean selectFromTripleStore = false;

	PrefixMapping prefixMapping;

	// for all variables in the query we add their name and their type
	// for example if we have ?sVariable p1 ?oVariable
	// in the mapping we will add <<sVariable, s>, <oVariable, o>>
	private Map<String, String[]> mapping = new HashMap<String, String[]>();

	public SparkComplexTripleGroup(String tablename, PrefixMapping mapping, boolean selectFromTripleStore) {
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
		if (subject.isVariable()) {
			result.put(subject.getName(), new String[] { Tags.SUBJECT_COLUMN_NAME });
		}
		if (predicate.isVariable()) {
			selectFromTripleStore = true;
			result.put(predicate.getName(), new String[] { Tags.PREDICATE_COLUMN_NAME });
		}
		if (object.isVariable()) {
			if (selectFromTripleStore) {
				result.put(object.getName(), new String[] { Tags.OBJECT_COLUMN_NAME });
			} else {
				String objectString = object.getName();
				String predicateString = FmtUtils.stringForNode(predicate, prefixMapping);
				result.put(objectString, new String[] { SpecialCharFilter.filter(predicateString) });
			}
		}
		return result;
	}

	public SQLStatement translate() {
		SparkComplexSelect select = new SparkComplexSelect(this.name);
		select.setComplexColumns(new HashMap<String, Boolean>(ComplexPropertyTableColumns.getColumns()));

		ArrayList<String> vars = new ArrayList<String>();
		ArrayList<String> whereConditions = new ArrayList<String>();

		boolean subjectFirstCheck = true;
		for (int i = 0; i < triples.size(); i++) {
			Triple triple = triples.get(i);
			Node subject = triple.getSubject();
			Node predicate = triple.getPredicate();
			Node object = triple.getObject();

			/* Subject */
			if (subjectFirstCheck) {
				subjectFirstCheck = false;
				// only check subject once per group
				if (subject.isURI() || subject.isBlank()) {
					// subject is bound -> add to Filter
					String subjectString = FmtUtils.stringForNode(subject, this.prefixMapping);

					whereConditions.add(Tags.SUBJECT_COLUMN_NAME + " = '" + subjectString + "'");
				} else {
					// mark the subject as a variable
					vars.add(subject.getName());
					whereConditions.add(Tags.SUBJECT_COLUMN_NAME + " IS NOT NULL ");
				}
			}

			/* Predicate */
			if (predicate.isURI()) {
				// predicate is bound -> add to Filter
				String predicateString = FmtUtils.stringForNode(predicate, this.prefixMapping);
				whereConditions.add(SpecialCharFilter.filter(predicateString) + " IS NOT NULL");
			} else {
				// mark the predicate as a variable
				String predicateString = predicate.getName();
				vars.add(predicateString);
			}

			/* Object */
			if (object.isURI() || object.isLiteral() || object.isBlank()) {
				String stringObject = FmtUtils.stringForNode(object, this.prefixMapping);

				if (object.isLiteral()) {
					stringObject = "" + object.getLiteral().getValue();
				}
				String predicateString = FmtUtils.stringForNode(predicate, this.prefixMapping);
				String predicateStringFiltered = SpecialCharFilter.filter(predicateString);
				String condition = "";
				if (selectFromTripleStore) {
					condition = Tags.OBJECT_COLUMN_NAME + " = '" + stringObject + "'";
					// is complex property search using array_contains function
					// array_contains(<complex column>,<searchedString>)
				} else if (ComplexPropertyTableColumns.getColumns().get(predicateStringFiltered)) {
					condition = "array_contains(" + predicateStringFiltered + ", '" + stringObject + "')";
				} else {
					condition = predicateStringFiltered + " = '" + stringObject + "'";
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

	public Map<String, String[]> getMappings() {
		Map<String, String[]> temp = new HashMap<String, String[]>();
		temp.putAll(this.mapping);
		return temp;
	}

	public void setMapping(Map<String, String[]> mapping) {
		this.mapping = mapping;
	}
}
