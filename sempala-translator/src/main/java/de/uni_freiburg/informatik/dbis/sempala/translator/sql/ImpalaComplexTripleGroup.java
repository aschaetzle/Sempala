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
 * Group of triples within a BGP (same subject). Every Triple Group when translated produces a single query.
 * It exposes information about the variables that could be joined with other groups.
 *
 * @author Matteo Cossu
 *
 */
public class ImpalaComplexTripleGroup {

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

	public ImpalaComplexTripleGroup(String tablename, PrefixMapping mapping,
			boolean selectFromTripleStore) {
		this.name = tablename;
		this.prefixMapping = mapping;
		this.selectFromTripleStore = selectFromTripleStore;
	}

	public void add(Triple triple) {
		triples.add(triple);
		mapping.putAll(getMappingVarsOfTriple(triple));

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
	
	/**
	 * From a triple the method extracts the variable names 
	 * and the columns to be selected from the table referring to that variable.
	 * 
	 * @param t
	 * @return
	 */
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
				String predicateString = getPropertyFromURI(FmtUtils
						.stringForNode(predicate, prefixMapping), false);
				
				
				result.put(objectString, new String[] { SpecialCharFilter
						.filter(predicateString) });
			}
		}
		return result;
	}

	
	/*
	 * Produce the SQL query for the TripleGroup
	 */
	public SQLStatement translate() {
		ImpalaComplexSelect select = new ImpalaComplexSelect(this.name);
		
		// give the information about the complex columns to the select
		select.setComplexColumns(new HashMap<String,Boolean>(ComplexPropertyTableColumns.getColumns()));
		
		// if one of the properties is a complex one, set the select accordingly
		for (int i = 0; i < triples.size(); i++) {
			String predicateString = getPropertyFromURI(FmtUtils
					.stringForNode(triples.get(i).getPredicate(), prefixMapping), false);
			if(select.is_complex_column.get(predicateString)){
					select.setComplexVariables();
					break;
			}
		
		}
		
		ArrayList<String> vars = new ArrayList<String>();
		ArrayList<String> whereConditions = new ArrayList<String>();
		boolean first = true;
		
		// for each triple collect variables and nodes to be selected or
		// filtered in the query
		for (int i = 0; i < triples.size(); i++) {
			Triple triple = triples.get(i);
			Node subject = triple.getSubject();
			Node predicate = triple.getPredicate();
			Node object = triple.getObject();
			if (first) {
				first = false;
				// only check subject once per group
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
				boolean deleteActualTriple = false;
				
				// the same predicate is requested twice -> need to have CROSS JOINS 
				int index = searchTripleSamePredicate(i);
				while (index != -1) {
					
					Triple otherTriple = triples.get(index);
					Node otherObject = otherTriple.getObject();
					Node otherPredicate = otherTriple.getPredicate();
					
					// if a duplicate triple is found and is not a variable
					// delete it from the list but add it to the cross join list
					if (otherObject.isURI() || otherObject.isLiteral() || otherObject.isBlank()){
						String objectString =  getPropertyFromURI(
								FmtUtils.stringForNode(otherObject, this.prefixMapping), true)	;
						String predString = getPropertyFromURI(FmtUtils
								.stringForNode(otherPredicate, prefixMapping), false);
						if (select.crossProperties.containsKey(predString))
							select.crossProperties.get(predString).add(objectString);
						else
							select.crossProperties.put(predString, new ArrayList<String>(Arrays.asList(objectString)));
						triples.remove(index);	
					}
					// if the actual triple is not a variable 
					// delete it from the list but add it to the cross join list
					if (object.isURI() || object.isLiteral() || object.isBlank()){
						// add to the special list to be crossjoined
						String objectString =  getPropertyFromURI(
								FmtUtils.stringForNode(object, this.prefixMapping), true);
						
						String predString = getPropertyFromURI(FmtUtils
								.stringForNode(predicate, prefixMapping), false);
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
				if(deleteActualTriple){
					i--;
					continue;
				}
					
				// predicate is bound -> add to Filter
				String predicateString = getPropertyFromURI(FmtUtils
						.stringForNode(predicate, this.prefixMapping), false);
				
				whereConditions.add(SpecialCharFilter.filter(predicateString)
						+ " IS NOT NULL");

			} else {
				String predicateString = predicate.getName();
				vars.add(predicateString);
			}
			
			if (object.isURI() || object.isLiteral() || object.isBlank()) {
				String string =  getPropertyFromURI(FmtUtils.stringForNode(object,
						this.prefixMapping),  true);
				
				/**
				 * this is commented because getValue() of a literal removes the language tag
				 * that is instead registered in the input complex property table.
				 * Could be solved in a more general way in future.
				if (object.isLiteral()) {
					string = "" + object.getLiteral().getValue();
				} 
				*/
				
				String condition = "";
				if (selectFromTripleStore) {
					condition = Tags.OBJECT_COLUMN_NAME + " = '"
							+ string + "'";
				} else {
					String predicateString = getPropertyFromURI((FmtUtils
							.stringForNode(predicate, this.prefixMapping)), false);
					
					condition = predicateString	+ " = '" + string + "'";
				}
				whereConditions.add(condition);
			} else { 
				// then the triple object is a variable
				vars.add(object.getName());
			}
		}
		// SELECT (list of variables)
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

	public int getSharedVars(ImpalaComplexTripleGroup other) {
		return JoinUtil.getSharedVars(this.mapping, other.getMappings()).size();
	}

	/**
	 * Join with another TripleGroup.
	 */

	public void join(ImpalaComplexTripleGroup other) {
		for (String entry : other.mapping.keySet()) {
			if (!mapping.containsKey(entry)) {
				mapping.put(entry, other.mapping.get(entry));
			}
		}

	}

	// search for another triple with the same predicate
	// return the index if exists
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

	/*
	 * retrieve the mapping, the variables selected with 
	 * the corresponding column name
	 */
	public Map<String, String[]> getMappings() {
		Map<String, String[]> temp = new HashMap<String, String[]>();
		temp.putAll(this.mapping);
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
