package de.uni_freiburg.informatik.dbis.sempala.translator.sql;

import java.util.ArrayList;
import java.util.Arrays;
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
				String predicateString = getPropertyFromURI(FmtUtils
						.stringForNode(predicate, prefixMapping), false);
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

				boolean deleteActualTriple = false;

				// the same predicate is requested twice -> need to have CROSS
				// JOINS
				int index = searchTripleSamePredicate(i);
				while (index != -1) {

					Triple otherTriple = triples.get(index);
					Node otherObject = otherTriple.getObject();
					Node otherPredicate = otherTriple.getPredicate();

					// if a duplicate triple is found and is not a variable
					// delete it from the list but add it to the cross join list
					if (otherObject.isURI() || otherObject.isLiteral() || otherObject.isBlank()) {
						String objectString = getPropertyFromURI(
								FmtUtils.stringForNode(otherObject, this.prefixMapping), true);
						String predString = getPropertyFromURI(FmtUtils.stringForNode(otherPredicate, prefixMapping),
								false);
						if (select.crossProperties.containsKey(predString))
							select.crossProperties.get(predString).add(objectString);
						else
							select.crossProperties.put(predString, new ArrayList<String>(Arrays.asList(objectString)));
						triples.remove(index);
					}
					// if the actual triple is not a variable
					// delete it from the list but add it to the cross join list
					if (object.isURI() || object.isLiteral() || object.isBlank()) {
						// add to the special list to be crossjoined
						String objectString = getPropertyFromURI(FmtUtils.stringForNode(object, this.prefixMapping),
								true);

						String predString = getPropertyFromURI(FmtUtils.stringForNode(predicate, prefixMapping), false);
						if (select.crossProperties.containsKey(predString))
							select.crossProperties.get(predString).add(objectString);
						else
							select.crossProperties.put(predString, new ArrayList<String>(Arrays.asList(objectString)));
						triples.remove(i);
						deleteActualTriple = true;
						break;
					}

					index = searchTripleSamePredicate(i);

				}
				// if actual triple was deleted, adjust the loop accordingly
				// otherwise one element will be skipped
				if (deleteActualTriple) {
					i--;
					continue;
				}

				// predicate is bound -> add to Filter
				String predicateString = getPropertyFromURI(FmtUtils.stringForNode(predicate, this.prefixMapping), false);
				whereConditions.add(SpecialCharFilter.filter(predicateString) + " IS NOT NULL");
			} else {
				// mark the predicate as a variable
				String predicateString = predicate.getName();
				vars.add(predicateString);
			}

			/* Object */
			if (object.isURI() || object.isLiteral() || object.isBlank()) {
				String stringObject = getPropertyFromURI(FmtUtils.stringForNode(object,
						this.prefixMapping),  true);
				String predicateString =  getPropertyFromURI((FmtUtils
						.stringForNode(predicate, this.prefixMapping)), false);
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
			select.setFrom(Tags.CACHED_COMPLEX_PROPERTYTABLE_TABLENAME);
		}
		// WHERE
		for (String where : whereConditions) {
			select.addWhereConjunction(where);
		}

		return select;
	}

	/*
	 * Jena tries to return full URI, even if prefix are not used 
	 * it tries to add itself a path that is unknown in the loading phase.
	 * The function solves this problem by extracting the correct values from
	 * the queries.
	 */
	private String getPropertyFromURI(String uri, boolean isNotColumnName) {
		String property = uri;
		
		// strip the base of the URI
		if (uri.contains("/")){
			String[] splitted = uri.split("/");
			property = splitted[splitted.length - 1];
			
			// return the object with the brackets if is not a property
			if (property.endsWith(">") && isNotColumnName)
				return "<" + property;
		}
		
		// safety check
		if (isNotColumnName)
			return property;
		
		// strip the brackets if present
		if(property.startsWith("<") && !isNotColumnName)
			property = property.substring(1);
		if(property.endsWith(">") && !isNotColumnName)
			property = property.substring(0, property.length() -1);
		
		// transform  all the invalid characters with underscore
		return SpecialCharFilter.filter(property);
		
	}
	
	// search for another triple with the same predicate
	// return the index if exists
	public int searchTripleSamePredicate(int index) {
		for (int i = 0; i < this.triples.size(); i++) {
			if (i != index && triples.get(index).getPredicate().equals(triples.get(i).getPredicate())) {

				return i;
			}
		}

		return -1;
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
