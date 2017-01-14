package de.uni_freiburg.informatik.dbis.sempala.translator.op;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;

import de.uni_freiburg.informatik.dbis.sempala.translator.sql.SQLStatement;

public class ImpalaBgpExtVPMultiTable extends ImpalaBGP {

	private Map<Triple, List<Node>> varIndex;
	private Map<Node, List<Triple>> invertedVarIndex;
	private Map<Triple, List<String>> ListOfExtVPTables;

	public ImpalaBgpExtVPMultiTable(OpBGP opBGP, PrefixMapping prefixes) {
		super(opBGP, prefixes);
		// TODO Auto-generated constructor stub
	}

	@Override
	public SQLStatement translate(String resultName) {

		// TODO Auto-generated method stub

		this.resultName = resultName;
		List<Triple> unhandledTriples = opBGP.getPattern().getList();

		// empty PrefixMapping when prefixes should be expanded
		if (expandPrefixes)
			prefixes = PrefixMapping.Factory.create();

		ListOfExtVPTables = new HashMap<>();
		for(int i=0;i<=unhandledTriples.size();i++){
			for(int j=i;j<unhandledTriples.size();j++){
				if(j!=i){
					CompareTriples(unhandledTriples.get(i),unhandledTriples.get(j));
				}
			}	
		}
		
		
		
		// Build variable index
//		varIndex = new HashMap<>();
//		for (Triple triple : unhandledTriples)
//			for (Node node : Arrays.asList(triple.getSubject(), triple.getPredicate(), triple.getObject()))
//				if (node.isVariable()) {
//					if (!varIndex.containsKey(triple))
//						varIndex.put(triple, new ArrayList<Node>());
//					varIndex.get(triple).add(node);
//				}
//		Iterator itVarIndex = invertedVarIndex.keySet().iterator();
//		while (itVarIndex.hasNext()) {
//			System.out.println(itVarIndex.next());
//		}

//		System.out.println(varIndex.keySet());
		// Build inverted variable index
//		invertedVarIndex = new HashMap<>();
//		for (Entry<Triple, List<Node>> entry : varIndex.entrySet())
//			for (Node varNode : entry.getValue()) {
//				if (!invertedVarIndex.containsKey(varNode))
//					invertedVarIndex.put(varNode, new ArrayList<Triple>());
//				invertedVarIndex.get(varNode).add(entry.getKey());
//			}

		
//		Iterator itInvVarIndex = invertedVarIndex.keySet().iterator();
		
//		while (itInvVarIndex.hasNext()) {
//			Object key = itInvVarIndex.next();
//			int NrOfTriples = invertedVarIndex.get(key).size();
//			
//			for (Triple triple : invertedVarIndex.get(key)) {
//				if(NrOfTriples == 1){
//					//Select directly partition with p=predicate
//					System.out.println(key + "-" + triple.getPredicate());
//				}
//				else{
//					
////					if(triple.getSubject().equals(key)){
////						System.out.println(key + "-" + triple.getSubject()+" "+triple.getPredicate()+"");
////					}
////					else{
////						System.out.println(key + "-" + triple.getObject());
////					}
//					
//				}
//				System.out.println(key + "-"
//						+ triple.getSubject() + " " + triple.getPredicate() + " " + triple.getObject());
//			}
//			System.out.println("-----------------");
//		}		
		
		
//		while (itInvVarIndex.hasNext()) {
//			Object key = itInvVarIndex.next();
//			int NrOfTriples = invertedVarIndex.get(key).size();
//			
//			if(NrOfTriples == 1){
//				Triple triple = invertedVarIndex.get(key).iterator().next();
//				System.out.println("VPTable P=" + triple.getPredicate());
//			}
//			else{
//				for (Triple triple : invertedVarIndex.get(key)) {
//					
//					System.out.println(key + "-"
//							+ triple.getSubject() + " " + triple.getPredicate() + " " + triple.getObject());
//				}
//			}
//			System.out.println("-----------------");		
//		}
//		
		
		Iterator it = ListOfExtVPTables.keySet().iterator();
		while (it.hasNext()) {
			Object key = it.next();
			System.out.println(ListOfExtVPTables.get(key));
		}
		
		
		
		System.out.println("ExtVPMultiTable Class Translator");
		return null;
	}

	public void CompareTriples(Triple T1, Triple T2) {
		ArrayList<String> ExtVpsT1 = new ArrayList<String>();
		ArrayList<String> ExtVpsT2 = new ArrayList<String>();
		if (T1.getSubject().equals(T2.getSubject())) {
			// Insert for key=t1 extvp_p1_p2_SS and for key=t2 extvp_p2_p1_SS
			ExtVpsT1.add(String.format("extvp_%s_%s_SS", T1.getPredicate(), T2.getPredicate()));
			ExtVpsT2.add(String.format("extvp_%s_%s_SS", T2.getPredicate(), T1.getPredicate()));
		} else if (T1.getSubject().equals(T2.getObject())) {
			// Insert for key=t1 extvp_p1_p2_SO and for key=t2 extvp_p2_p1_OS
			ExtVpsT1.add(String.format("extvp_%s_%s_SO", T1.getPredicate(), T2.getPredicate()));
			ExtVpsT2.add(String.format("extvp_%s_%s_OS", T2.getPredicate(), T1.getPredicate()));
		} else if (T1.getObject().equals(T2.getSubject())) {
			// Insert for key=t1 extvp_p1_p2_OS and for key=t2 extvp_p2_p1_SO
			ExtVpsT1.add(String.format("extvp_%s_%s_OS", T1.getPredicate(), T2.getPredicate()));
			ExtVpsT2.add(String.format("extvp_%s_%s_SO", T2.getPredicate(), T1.getPredicate()));
		} else if (T1.getObject().equals(T2.getObject())) {
			// Insert for key=t1 extvp_p1_p2_OO and for key=t2 extvp_p2_p1_OO
			ExtVpsT1.add(String.format("extvp_%s_%s_OO", T1.getPredicate(), T2.getPredicate()));
			ExtVpsT2.add(String.format("extvp_%s_%s_OO", T2.getPredicate(), T1.getPredicate()));
		}
		if (!ListOfExtVPTables.containsKey(T1))
			ListOfExtVPTables.put(T1, ExtVpsT1);
		else {
			ArrayList<String>Temp = (ArrayList<String>) ListOfExtVPTables.get(T1);
			ExtVpsT1.addAll(Temp);
			ListOfExtVPTables.remove(T1);
			ListOfExtVPTables.put(T1, ExtVpsT1);
		}
		if (!ListOfExtVPTables.containsKey(T2))
			ListOfExtVPTables.put(T2, ExtVpsT2);
		else {
			ArrayList<String>Temp = (ArrayList<String>) ListOfExtVPTables.get(T2);
			ExtVpsT2.addAll(Temp);
			ListOfExtVPTables.remove(T2);
			ListOfExtVPTables.put(T2, ExtVpsT2);
		}
	}
}
