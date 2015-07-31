package sparql2impala.mapreduce.util;

import java.util.ArrayList;

public class ParseResult {

	private String nodeName;
	private ArrayList<String[]> arcs = new ArrayList<String[]>();

	public ParseResult(String nodeName, ArrayList<String[]> arcs) {
		super();
		this.nodeName = nodeName;
		this.arcs = arcs;
	}
	

	public String getNodeName() {
		return nodeName;
	}

	public ArrayList<String[]> getArcs() {
		return arcs;
	}

	public void addArc(String[] arc) {
		arcs.add(arc);
	}

}
