package de.uni_freiburg.informatik.dbis.sempala.translator;


import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.Transformer;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformFilterConjunction;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformFilterDisjunction;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformFilterEquality;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformFilterPlacement;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformJoinStrategy;

import de.uni_freiburg.informatik.dbis.sempala.translator.op.ImpalaOp;
import de.uni_freiburg.informatik.dbis.sempala.translator.sparql.AlgebraTransformer;
import de.uni_freiburg.informatik.dbis.sempala.translator.sparql.BGPOptimizerNoStats;
import de.uni_freiburg.informatik.dbis.sempala.translator.sparql.TransformFilterVarEquality;


/**
 * Main Class of the ImpalaSPARQL translator.
 * This class generates the Algebra Tree for a SPARQL query,
 * performs some optimizations, translates the Algebra tree into an
 * ImpalaOp tree, initializes the translation and collects the final output.
 *
 * @author Antony Neu
 */
public class Translator {

	/** The input file containing the sparql query */
	private String inputFile = null;

	/** The format to use to build the output queries against */
	private Format format = Format.SINGLETABLE;

	/** Indicates if prefixes in the RDF Triples should be expanded or not */
	private boolean expandPrefixes = false;

	/** Indicates if optimizations of the SPARQL Algebra are enabled */
	private boolean optimizer = false;

	/** Indicates if BGP optimizations are enabled */
	private boolean bgpOptimizer = true;

	/** Indicates if join optimizations are enabled */
	private boolean joinOptimizer = false;

	/** Indicates if filter optimizations are enabled */
	private boolean filterOptimizer = true;
	
	/** The value of threshold*/
	public static double threshold;
	
	/** The value of threshold*/
	public static boolean StraighJoin = false;
	
	/** The value of result table name */
	public String result_table_name = "extvp";
	
	// Define a static logger variable so that it references the corresponding Logger instance
	private static final Logger logger = Logger.getLogger(Translator.class);

	/**
	 * Translates the SPARQL query into a sql script for use with Impala.
	 */
	public String translateQuery() {

		assert ! ( inputFile!=null ) : "in- and output must not be null!";

		//Parse input query
		Query query = QueryFactory.read("file:"+inputFile);

		//Get prefixes defined in the query
		PrefixMapping prefixes = query.getPrefixMapping();

		//Generate translation logfile
		PrintWriter logWriter;
		try {
			logWriter = new PrintWriter(inputFile + ".log");
		} catch (FileNotFoundException ex) {
			logger.warn("Cannot open translation logfile, using stdout instead!", ex);
			logWriter = new PrintWriter(System.out);
		}

		//Output original query to log
		logWriter.println("SPARQL Input Query:");
		logWriter.println("###################");
		logWriter.println(query);
		logWriter.println();
		//Print Algebra Using SSE, true -> optimiert
		//PrintUtils.printOp(query, true);

		//Generate Algebra Tree of the SPARQL query
		Op opRoot = Algebra.compile(query);

		//Output original Algebra Tree to log
		logWriter.println("Algebra Tree of Query:");
		logWriter.println("######################");
		logWriter.println(opRoot.toString(prefixes));
		logWriter.println();

		//Optimize Algebra Tree if optimizer is enabled
		if(optimizer) {
			/*
			 * Algebra Optimierer führt High-Level Transformationen aus (z.B. Filter Equalilty)
			 * -> nicht BGP reordering
			 *
			 * zunächst muss gesetzt werden was alles optimiert werden soll, z.B.
			 * ARQ.set(ARQ.optFilterEquality, true);
			 * oder
			 * ARQ.set(ARQ.optFilterPlacement, true);
			 *
			 * Danach kann dann Algebra.optimize(op) aufgerufen werden
			 */

			/*
			 * Algebra.optimize always executes TransformJoinStrategy -> not always wanted
			 * ARQ.set(ARQ.optFilterPlacement, false);
			 * ARQ.set(ARQ.optFilterEquality, true);
			 * ARQ.set(ARQ.optFilterConjunction, true);
			 * ARQ.set(ARQ.optFilterDisjunction, true);
			 * opRoot = Algebra.optimize(opRoot);
			 */

			/*
			 * Reihenfolge der Optimierungen wichtig!
			 *
			 * 1. Transformationen der SPARQL-Algebra bis auf FilterPlacement -> könnte Kreuzprodukte erzeugen
			 * 2. BGPOptimizer -> Neuanordnung der Triple im BGP zur Vermeidung von Kreuzprodukten und zur Minimierung von Joins
			 * 3. FilterPlacement -> Vorziehen des Filters soweit möglich
			 */

			if(joinOptimizer) {
				TransformJoinStrategy joinStrategy = new TransformJoinStrategy();
				opRoot = Transformer.transform(joinStrategy, opRoot);
			}

			if(filterOptimizer) {
				//ARQ optimization of Filter conjunction
				TransformFilterConjunction filterConjunction = new TransformFilterConjunction();
				opRoot = Transformer.transform(filterConjunction, opRoot);

				//ARQ optimization of Filter disjunction
				TransformFilterDisjunction filterDisjunction = new TransformFilterDisjunction();
				opRoot = Transformer.transform(filterDisjunction, opRoot);

				//ARQ optimization of Filter equality
				TransformFilterEquality filterEquality = new TransformFilterEquality();
				opRoot = Transformer.transform(filterEquality, opRoot);

				//Own optimization of Filter variable equality
				TransformFilterVarEquality filterVarEquality = new TransformFilterVarEquality();
				opRoot = filterVarEquality.transform(opRoot);
			}

			if(bgpOptimizer) {
				//Own BGP optimizer using variable counting heuristics
				BGPOptimizerNoStats bgpOptimizer = new BGPOptimizerNoStats();
				opRoot = bgpOptimizer.optimize(opRoot);
			}

			if(filterOptimizer) {
				//ARQ optimization of Filter placement
				TransformFilterPlacement filterPlacement = new TransformFilterPlacement();
				opRoot = Transformer.transform(filterPlacement, opRoot);
			}

			//Output optimized Algebra Tree to log file
			logWriter.println("optimized Algebra Tree of Query:");
			logWriter.println("################################");
			logWriter.println(opRoot.toString(prefixes));
			logWriter.println();
		}

		//Transform SPARQL Algebra Tree in ImpalaOp Tree
		AlgebraTransformer transformer = new AlgebraTransformer(prefixes, format);
		ImpalaOp impalaOpRoot = transformer.transform(opRoot);

		// Print ImpalaOp Tree to log
		logWriter.println("ImpalaOp Tree:");
		logWriter.println("###########");
		ImpalaOpPrettyPrinter.print(logWriter, impalaOpRoot);
		logWriter.println();

		// Walk through ImpalaOp Tree and generate translation

		// Translate the query
		ImpalaOpTranslator algebraTranslator = new ImpalaOpTranslator();
		String sqlString = algebraTranslator.translate(impalaOpRoot, expandPrefixes);

		//Output original Algebra Tree to log
		logWriter.println("SQL Output Query:");
		logWriter.println("######################");
		logWriter.println(sqlString);
		//close log file
		logWriter.close();

		return sqlString;
	}


	/*
	 * Getters and setters. See members descriptions for semantics
	 */


	public String inputFile() {
		return inputFile;
	}

	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}

	public boolean expandPrefixes() {
		return expandPrefixes;
	}

	public void setExpandPrefixes(boolean expandPrefixes) {
		this.expandPrefixes = expandPrefixes;
	}

	public boolean optimizer() {
		return optimizer;
	}

	public void setOptimizer(boolean value) {
		this.optimizer = value;
	}

	public boolean bgpOptimizer() {
		return bgpOptimizer;
	}

	public void setBgpOptimizer(boolean bgpOptimizer) {
		this.bgpOptimizer = bgpOptimizer;
	}

	public boolean joinOptimizer() {
		return joinOptimizer;
	}

	public void setJoinOptimizer(boolean value) {
		this.joinOptimizer = value;
	}

	public boolean isFilterOptimizer() {
		return filterOptimizer;
	}

	public void setFilterOptimizer(boolean value) {
		this.filterOptimizer = value;
	}

	public Format getFormat() {
		return format;
	}

	public void setFormat(Format format) {
		this.format = format;
	}
	
	public void setStraightJoin(boolean StraightJoin) {
		this.StraighJoin = StraightJoin;
	}
	
	public void setThreshold(String Threshold) {
		try {
			threshold = Double.parseDouble(Threshold);
			if (threshold <= 0)
				throw new IllegalArgumentException();
		} catch (Exception e) {
			System.out.print(String.format("Threshold '%s' is not a proper value as threshold", threshold));
			System.exit(1);
		}
	}
	
}