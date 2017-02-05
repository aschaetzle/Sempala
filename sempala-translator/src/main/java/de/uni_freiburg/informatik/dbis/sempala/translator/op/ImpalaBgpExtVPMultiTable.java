package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.sql.ResultSet;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;

import de.uni_freiburg.informatik.dbis.sempala.translator.Tags;
import de.uni_freiburg.informatik.dbis.sempala.translator.run.Main;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.Join;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.JoinType;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;
import de.uni_freiburg.informatik.dbis.sempala.translator.sql.Select;
import de.uni_freiburg.informatik.dbis.sempala.translator.Translator;

/**
*
* @author Lis Bakalli <bakallil@informatik.uni-freiburg.de>
*
*/

public class ImpalaBgpExtVPMultiTable extends ImpalaBGP {

	private Map<Triple, List<String>> ListOfExtVPTables;
	private Map<Triple, String> ListOfExtVPTriples = new HashMap<Triple, String>();
	private Map<String, List<String>> invertedVarIndex;
	private List<Triple> QueryTriples;
	double Threshold = 1.0;	

	public ImpalaBgpExtVPMultiTable(OpBGP opBGP, PrefixMapping prefixes) {
		super(opBGP, prefixes);
	}

	@Override
	public SQLStatement translate(String resultName) {
		long startTimeFindExtvp = System.currentTimeMillis();
		this.resultName = resultName;

		setThreshold(Translator.threshold);
		QueryTriples = opBGP.getPattern().getList();
		// empty PrefixMapping when prefixes should be expanded
		if (expandPrefixes)
			prefixes = PrefixMapping.Factory.create();

		// Get list of possible ExtVP tables for each triple.
		ListOfExtVPTables = new HashMap<>();
		for (int i = 0; i <= QueryTriples.size(); i++) {
			for (int j = i; j < QueryTriples.size(); j++) {
				CompareTriples(QueryTriples.get(i), i, QueryTriples.get(j), j);
			}
		}
		// Find the best possible table for each corresponding triple.
		Iterator<Triple> it = ListOfExtVPTables.keySet().iterator();
		while (it.hasNext()) {
			double min_sel = 1;
			double SF = 1;
			Triple key = it.next();
			String selected_extvp_table = Tags.TABLENAME_TRIPLE_TABLE;
			for (int i = 0; i < ListOfExtVPTables.get(key).size(); i++) {
				String Extvptable_Triple = ListOfExtVPTables.get(key).get(i);
				if (!Extvptable_Triple.startsWith("extvp_")) {
					if (min_sel == 1) {
						String URIPredicate = Extvptable_Triple;
						int index = URIPredicate.lastIndexOf('#');
						if (index == -1)
							index = URIPredicate.lastIndexOf('/');
						String URI = URIPredicate.substring(0, index + 1);
						String Pred = URIPredicate.substring(index + 1, URIPredicate.length());
						String Prefix = prefixes.getNsURIPrefix(URI);
						if (Prefix == null)
							selected_extvp_table = URI + Pred;
						else
							selected_extvp_table = Prefix + ":" + Pred;
					}
				} else {
					int IndexOfTriple = Extvptable_Triple.indexOf(" ");
					String Extvptable = Extvptable_Triple.substring(0, IndexOfTriple);
					 if(IsEmpty(Extvptable))
						 break;
					String ExtvptableType = Extvptable.substring(Extvptable.length() - 2, Extvptable.length());
					String Query = String.format(
							"SELECT extvptable_sf FROM extvp_tableofstats_%s WHERE extvptable_name = '%s';",
							ExtvptableType, Extvptable);
					try {
						ResultSet result = Main.impalaConnection.createStatement().executeQuery(Query);
						result.next();
						SF = Double.parseDouble(result.getString("extvptable_sf"));
						if (SF <= Threshold && SF < min_sel) {
							selected_extvp_table = Extvptable_Triple;
							min_sel = SF;
						}
					} catch (Exception e) {
						break;
					}
				}
			}
			ListOfExtVPTriples.put(key, selected_extvp_table);
		}

		// Create Joins between best ExtVP tables.
		SQLStatement result = MakeJoins();
		System.out.print(String.format("Create query with extvp tables: %s ms\n",
				System.currentTimeMillis() - startTimeFindExtvp));
		return result;
	}

	/**
	 * Create On Conditions for join between ExtVP tables depending on the triples.
	 *  
	 * @param TripleConditions - Use Conditions between triples to build the inverted index of variables.
	 * @return
	 */
	private List<String> CreateonConditions(Map<String, List<String>> TripleConditions) {
		// Build inverted variable index
		invertedVarIndex = new HashMap<>();
		for (Entry<String, List<String>> entry : TripleConditions.entrySet())
			for (String varNode : entry.getValue()) {
				if (!invertedVarIndex.containsKey(varNode))
					invertedVarIndex.put(varNode, new ArrayList<String>());
				invertedVarIndex.get(varNode).add(entry.getKey());
			}

		List<String> onCondition = new ArrayList<>();
		Iterator<String> it = invertedVarIndex.keySet().iterator();
		while (it.hasNext()) {
			String Variable = it.next();
			if (invertedVarIndex.get(Variable).size() > 1) {
				for (int i = 0; i < invertedVarIndex.get(Variable).size(); i++) {
					for (int j = i; j < invertedVarIndex.get(Variable).size(); j++) {
						if (i != j) {
							onCondition.add(String.format("%s.%s=%s.%s", invertedVarIndex.get(Variable).get(i),
									Variable, invertedVarIndex.get(Variable).get(j), Variable));
						}
					}
				}
			}
		}
		return onCondition;
	}
	
	/**
	 * Get only the ExtVP table or triple table without the corresponding triple of join.
	 * 
	 * @param Table - Table which is modified.
	 * @return
	 */
	private String RemoveTripleNumber(String Table) {
		if (Table.startsWith("extvp_")) {
			int IndexOfTriple = Table.indexOf(" ");
			String Extvptable = Table.substring(0, IndexOfTriple);
			return Extvptable;
		} else {
			return Table;
		}
	}

	/**
	 * Returns the statement of BGP with join between ExtVP tables.
	 * 
	 * @return
	 */
	private SQLStatement MakeJoins() {
		SQLStatement first = null;
		List<SQLStatement> rights = new ArrayList<>();
		Map<String, List<String>> TripleConditions = new HashMap<String, List<String>>();

		for (int i = 0; i < QueryTriples.size(); i++) {
			List<String> onConditions = new ArrayList<>();
			Select stmt = new Select("T" + String.valueOf(i));
			Triple T = QueryTriples.get(i);
			String From = RemoveTripleNumber(ListOfExtVPTriples.get(T));
			if (!From.startsWith("extvp_")) {
				stmt.addWhereConjunction(Tags.PREDICATE_COLUMN_NAME + "='" + From + "'");
				stmt.setFrom(Tags.TABLENAME_TRIPLE_TABLE);
			} else
				stmt.setFrom(From);
			if (T.getSubject().isLiteral())
				stmt.addWhereConjunction(Tags.SUBJECT_COLUMN_NAME + "=" + T.getSubject().toString().replace("\"", "\'"));
			else {
				stmt.addSelector(T.getSubject().getName(), new String[] { Tags.SUBJECT_COLUMN_NAME });
				onConditions.add(T.getSubject().getName());
			}
			if (T.getObject().isLiteral())
				stmt.addWhereConjunction(Tags.OBJECT_COLUMN_NAME + "=" + T.getObject().toString().replace("\"", "\'"));
			else {
				stmt.addSelector(T.getObject().getName(), new String[] { Tags.OBJECT_COLUMN_NAME });
				onConditions.add(T.getObject().getName());
			}
			if (i == 0)
				first = stmt;
			else
				rights.add(stmt);
			TripleConditions.put("T" + String.valueOf(i), onConditions);
		}
		Join join = new Join("extvp", first, rights, CreateonConditions(TripleConditions), JoinType.INNEREXTVP);

		this.resultName = join.getName();
		for (String var : invertedVarIndex.keySet())
			this.resultSchema.put(var, new String[0]);

		return join;
	}
	
	/**
	 * Use defined prefixes for making predicates compatible with 
	 * ExtVP table naming.
	 * 
	 * @param Predicate - Predicate to be renamed.
	 * @return
	 */
	private String PrefixforExtVP(Node Predicate) {
		String URIPredicate = Predicate.toString();
		int index = URIPredicate.lastIndexOf('#');
		if (index == -1)
			index = URIPredicate.lastIndexOf('/');
		String URI = URIPredicate.substring(0, index + 1);
		String Pred = URIPredicate.substring(index + 1, URIPredicate.length());
		String Prefix = prefixes.getNsURIPrefix(URI);
		if(Prefix==null){
			String RenamedURI = URI.replaceAll("[<>/.`~,\\s\\-:\\?]", "_");
			return RenamedURI + Pred;
		}
		else
		return Prefix + "_" + Pred;
	}
	
	/**
	 * Compare two triples to see if they are joined by Subject or Object.
	 * Returns list of possible ExtVP tables to be seleced in query.
	 * 
	 * @param T1 - First triple.
	 * @param i - Position of first triple in list of triples.
	 * @param T2 - Second triple.
	 * @param j - Position of second triple in list of triples.
	 */
	private void CompareTriples(Triple T1, int i, Triple T2, int j) {
		ArrayList<String> ExtVpsT1 = new ArrayList<String>();
		ArrayList<String> ExtVpsT2 = new ArrayList<String>();
		ExtVpsT1.add(T1.getPredicate().toString());

		if (!T1.equals(T2)) {
			if (T1.getSubject().equals(T2.getSubject())) {
				ExtVpsT1.add(String.format("extvp_%s_%s_ss T%d", PrefixforExtVP(T1.getPredicate()),
						PrefixforExtVP(T2.getPredicate()), j));
				ExtVpsT2.add(String.format("extvp_%s_%s_ss T%d", PrefixforExtVP(T2.getPredicate()),
						PrefixforExtVP(T1.getPredicate()), i));
			}
			if (T1.getSubject().equals(T2.getObject())) {
				ExtVpsT1.add(String.format("extvp_%s_%s_so T%d", PrefixforExtVP(T1.getPredicate()),
						PrefixforExtVP(T2.getPredicate()), j));
				ExtVpsT2.add(String.format("extvp_%s_%s_os T%d", PrefixforExtVP(T2.getPredicate()),
						PrefixforExtVP(T1.getPredicate()), i));
			}
			if (T1.getObject().equals(T2.getSubject())) {
				ExtVpsT1.add(String.format("extvp_%s_%s_os T%d", PrefixforExtVP(T1.getPredicate()),
						PrefixforExtVP(T2.getPredicate()), j));
				ExtVpsT2.add(String.format("extvp_%s_%s_so T%d", PrefixforExtVP(T2.getPredicate()),
						PrefixforExtVP(T1.getPredicate()), i));
			}
			if (T1.getObject().equals(T2.getObject())) {
				ExtVpsT1.add(String.format("extvp_%s_%s_oo T%d", PrefixforExtVP(T1.getPredicate()),
						PrefixforExtVP(T2.getPredicate()), j));
				ExtVpsT2.add(String.format("extvp_%s_%s_oo T%d", PrefixforExtVP(T2.getPredicate()),
						PrefixforExtVP(T1.getPredicate()), i));
			}
		}
		if (!ListOfExtVPTables.containsKey(T1))
			ListOfExtVPTables.put(T1, ExtVpsT1);
		else {
			ArrayList<String> Temp = (ArrayList<String>) ListOfExtVPTables.get(T1);
			ExtVpsT1.addAll(Temp);
			ListOfExtVPTables.remove(T1);
			ListOfExtVPTables.put(T1, ExtVpsT1);
		}
		if (!ListOfExtVPTables.containsKey(T2))
			ListOfExtVPTables.put(T2, ExtVpsT2);
		else {
			ArrayList<String> Temp = (ArrayList<String>) ListOfExtVPTables.get(T2);
			ExtVpsT2.addAll(Temp);
			ListOfExtVPTables.remove(T2);
			ListOfExtVPTables.put(T2, ExtVpsT2);
		}
	}

	/**
	 * Read value of threshold from CLI.
	 */
	private void setThreshold(String threshold) {
		try {
			Threshold = Double.parseDouble(threshold);
			if (Threshold <= 0)
				throw new IllegalArgumentException();
		} catch (Exception e) {
			System.out.print(String.format("Threshold '%s' is not a proper value as threshold", threshold));
			System.exit(1);
		}
	}

	/**
	 * Define what happens when empty possible ExtVP tables are found.
	 * 
	 * @param ExtvpTable - ExtVP table to be checked if empty.
	 * @return
	 */
	private boolean IsEmpty(String ExtvpTable){
		double NrTuples=1;
		String Query = String.format(
				"SELECT COUNT(*) AS NrTuples FROM extvp_tableofstats_emptytable WHERE ExtVPTable_Name = '%s';", ExtvpTable);
		try {
			ResultSet result = Main.impalaConnection.createStatement().executeQuery(Query);
			result.next();
			NrTuples = Double.parseDouble(result.getString("NrTuples"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(NrTuples != 0)
			return true;
		else
			return false;
	}
}
