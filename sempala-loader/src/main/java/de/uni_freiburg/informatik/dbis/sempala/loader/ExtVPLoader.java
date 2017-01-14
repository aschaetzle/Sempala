package de.uni_freiburg.informatik.dbis.sempala.loader;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import de.uni_freiburg.informatik.dbis.sempala.loader.sql.CreateStatement;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.Impala;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.SelectStatement;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.CreateStatement.DataType;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.CreateStatement.FileFormat;

public final class ExtVPLoader extends Loader {

	/** The constructor */
	public ExtVPLoader(Impala wrapper, String hdfsLocation) {
		super(wrapper, hdfsLocation);
		tablename_output = "ExtVP";
	}

	private double SF = 1;
	private ArrayList<String> ExtVPTypes = new ArrayList<String>();
	private ArrayList<String> ListOfPredicates = new ArrayList<>();
	private String TT = "tripletable";
	private int FirstPredicate = 0;

	/**
	 * Creates Extended Vertical Partitioning tables from a triple table.
	 *
	 * The input table has to be in triple table format. The output will be a
	 * set of tables in format described in 'XXXXXXS2RDF: RDF Querying with
	 * SPARQL on Spark'.
	 *
	 * @throws SQLException
	 */
	@Override
	public void load() throws SQLException {

		// Get Threshold from CLI
		setThreshold(threshold);

		// Omit ExtVP formats given in CLI
		setExtVPTypes(extvp_types_selected);

		// Load the triple table
		long timestampTT = System.currentTimeMillis();
		buildTripleTable();
		AddStats("BUILD TRIPLETABLE"," TIME","","Time",0,0, (double) (System.currentTimeMillis() - timestampTT) / 1000);
		
		// GetListOfPredicates given by user
		setListOfPredicates(TT);
		
		FirstPredicate = GetCompletedPredicate(1);
		
		System.out.print(String.format("Creating %s from '%s' \n", "ExtVps", TT));
		long timestamptotal = System.currentTimeMillis();
		
		for (int i = FirstPredicate; i < ListOfPredicates.size(); i++) {
			String p1 = ListOfPredicates.get(i);
			SelectStatement leftstmt = SelectPartition(TT, p1);
			for (int j = i; j < ListOfPredicates.size(); j++) {
				String p2 = ListOfPredicates.get(j);
				SelectStatement rightstmt = SelectPartition(TT, p2);
				double PartitionSizeP1 = TableSize(TT,p1);
				double PartitionSizeP2 = TableSize(TT,p2);
				if (!ExtVPTypes.isEmpty()) {
					if (ExtVPTypes.contains("so") && ExtVPTypes.contains("os")) {
						Compute_SOandOS(TT, p1, p2, SF, leftstmt, rightstmt, PartitionSizeP1, PartitionSizeP2);
					} else if (ExtVPTypes.contains("so") && !ExtVPTypes.contains("os")) {
						Compute_SO(TT, p1, p2, SF, leftstmt, rightstmt, PartitionSizeP1, PartitionSizeP2);
					} else if (!ExtVPTypes.contains("so") && ExtVPTypes.contains("os")) {
						Compute_OS(TT, p1, p2, SF, leftstmt, rightstmt, PartitionSizeP1, PartitionSizeP2);
					} else {
					}
					if (ExtVPTypes.contains("ss")) {
						Compute_SS(TT, p1, p2, SF, leftstmt, rightstmt, PartitionSizeP1, PartitionSizeP2);
					}
					if (ExtVPTypes.contains("oo")) {
						Compute_OO(TT, p1, p2, SF, leftstmt, rightstmt, PartitionSizeP1, PartitionSizeP2);
					}
				} else {
					System.out.println("ExtVPTypes is empty");
				}
				PhaseCompleted(i, p1, j, p2);
			}
		}		
		System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamptotal) / 1000));

		AddStats("Complete_EXTVP_TABLES"," TIME","","Time",0,0, (double) (System.currentTimeMillis() - timestamptotal) / 1000);

		System.exit(-1);
	}

	/**
	 * 
	 * @param threshold
	 */
	private void setThreshold(String threshold) {
		try {
			SF = Double.parseDouble(threshold);
			if (SF <= 0)
				throw new IllegalArgumentException();
		} catch (Exception e) {
			System.out.print(String.format("Threshold '%s' is not a proper value as threshold", threshold));
			System.exit(1);
		}
	}

	private void setExtVPTypes(String SelectedTypes) {

		String[] ExtVPTypesSelected;
		if (SelectedTypes != "\\n") {
			try {
				ExtVPTypesSelected = SelectedTypes.toLowerCase().split("[/.,\\s\\-:\\?]");
				for (int i = 0; i < ExtVPTypesSelected.length; i++) {
					if (ExtVPTypesSelected[i].length() != 2
							|| (!ExtVPTypesSelected[i].contains("s") && !ExtVPTypesSelected[i].contains("o")))
						throw new IllegalArgumentException();
					else {
						if (!ExtVPTypes.contains(ExtVPTypesSelected[i]))
							ExtVPTypes.add(ExtVPTypesSelected[i]);
					}
				}
			} catch (Exception e) {
				System.out.print(String.format("'%s' is not a proper format of ExtVP types", SelectedTypes));
				System.exit(1);
			}
		} else {
			ExtVPTypes.add("ss");
			ExtVPTypes.add("so");
			ExtVPTypes.add("os");
			ExtVPTypes.add("oo");
		}
	}

	// LIST OF PREDICATES ADD PATH PROPERTY
	private void setListOfPredicates(String TripleTable) throws IllegalArgumentException, SQLException {
		if (path_of_list_of_predicates != "\\n") {
			// // Get the prefixes and remove braces from long format
			System.out.println(String.format("Path for list of predicates is given %s", path_of_list_of_predicates));
			try {
				BufferedReader br = new BufferedReader(new FileReader(path_of_list_of_predicates));
				for (String line; (line = br.readLine()) != null;) {
					ListOfPredicates.add(line);
				}
				br.close();
			} catch (IOException e) {
				System.err.println("[ERROR] Could not open list of predicates file. Reason: " + e.getMessage());
				System.exit(1);
			}
		} else {
			System.out.println("Get All predicates");
			ResultSet DataSet = impala.select(column_name_predicate).distinct().from(TripleTable).execute();
			while (DataSet.next()) {
				ListOfPredicates.add(DataSet.getString(column_name_predicate));
			}
		}	
		java.util.Collections.sort(ListOfPredicates);
	}

	// COMPUTE EXTVPs
	private void Compute_SO(String TT, String p1, String p2, double SF, SelectStatement leftstmt, SelectStatement rightstmt, double PartitionSizeP1, double PartitionSizeP2) throws IllegalArgumentException, SQLException {
		// CREATE TABLE ExtVP_p1_p2_so AS (SELECT t1.s, t1.o FROM (SELECT s, o
		// FROM TT WHERE p = p1) t1 LEFT SEMI JOIN (SELECT s, o FROM TT WHERE p
		// = p2) t2 ON t1.s=t2.o);
		// COMPUTE STATS ExtVP_p1_p2_so;
		String ExtVPFormat = "SO";
		String TableName_p1p2_SO = TableName(p1, p2, ExtVPFormat);
		String TableName_p2p1_SO = TableName(p2, p1, ExtVPFormat);
		
		System.out.print(String.format("Creating %s from '%s'", TableName_p1p2_SO, TT));
		long timestamp = System.currentTimeMillis();

		CreateStatement cstmtSO = CreateTable(p1, p2, ExtVPFormat);
		cstmtSO.execute();

		SelectStatement mainstmt = impala.select(String.format("t1.%s", column_name_subject));
		mainstmt.addProjection(String.format("t1.%s", column_name_object));
		mainstmt.from(String.format("(%s) t1", leftstmt));
		mainstmt.leftSemiJoin(String.format("(%s) t2", rightstmt),
				String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_object), false);

		impala.insertOverwrite(TableName_p1p2_SO).selectStatement(mainstmt).execute();
		System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
		impala.computeStats(TableName_p1p2_SO);

		if (!isEmpty(TableName_p1p2_SO)) {
			double ExtVPSize = TableSize(TableName_p1p2_SO);
			double Selectivity = ExtVPSize/PartitionSizeP1;
			AddStats(TableName_p1p2_SO,p1, p2, ExtVPFormat, ExtVPSize, PartitionSizeP1, Selectivity);
			if (Selectivity >= SF)
				impala.dropTable(TableName_p1p2_SO);
		} 
		else {
			impala.dropTable(TableName_p1p2_SO);
			StoreEmptyTables(p1, p2, ExtVPFormat);
		}

		if (p1 != p2) {
			// CREATE TABLE ExtVP_p2_p1_so AS (SELECT t2.s, t2.o FROM (SELECT s,
			// o
			// FROM TT WHERE p = p1) t1 RIGHT SEMI JOIN (SELECT s, o FROM TT
			// WHERE p
			// = p2) t2 ON t1.o=t2.s);
			// COMPUTE STATS ExtVP_p2_p1_so;
			System.out.print(String.format("Creating %s from '%s'", TableName_p2p1_SO, TT));
			timestamp = System.currentTimeMillis();
			CreateStatement cstmtSO2 = CreateTable(p2, p1, ExtVPFormat);
			cstmtSO2.execute();

			SelectStatement mainstmt2 = impala.select(String.format("t2.%s", column_name_subject));
			mainstmt2.addProjection(String.format("t2.%s", column_name_object));
			mainstmt2.from(String.format("(%s) t1", leftstmt));
			mainstmt2.rightSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_subject), false);

			impala.insertOverwrite(TableName_p2p1_SO).selectStatement(mainstmt2).execute();
			System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
			impala.computeStats(TableName_p2p1_SO);

			if (!isEmpty(TableName_p2p1_SO)) {
				double ExtVPSize = TableSize(TableName_p2p1_SO);
				double Selectivity = ExtVPSize/PartitionSizeP2;
				AddStats(TableName_p2p1_SO,p2, p1, ExtVPFormat, ExtVPSize, PartitionSizeP2, Selectivity);
				if (TableSize(TableName_p2p1_SO) / TableSize(TT, p2) >= SF)
					impala.dropTable(TableName_p2p1_SO);
			} else {
				impala.dropTable(TableName_p2p1_SO);
				StoreEmptyTables(p2, p1, ExtVPFormat);
			}
		}
	}

	private void Compute_OS(String TT, String p1, String p2, double SF, SelectStatement leftstmt, SelectStatement rightstmt, double PartitionSizeP1, double PartitionSizeP2) throws IllegalArgumentException, SQLException {
		// CREATE TABLE ExtVP_p1_p2_os AS (SELECT t1.s, t1.o FROM (SELECT s, o
		// FROM TT WHERE p = p1) t1 LEFT SEMI JOIN (SELECT s, o FROM TT WHERE p
		// = p2) t2 ON t1.o=t2.s);
		// COMPUTE STATS ExtVP_p1_p2_os;
		String ExtVPFormat = "OS";
		String TableName_p1p2_OS = TableName(p1, p2, ExtVPFormat);
		String TableName_p2p1_OS = TableName(p2, p1, ExtVPFormat);
		
		System.out.print(String.format("Creating %s from '%s'", TableName_p1p2_OS, TT));
		long timestamp = System.currentTimeMillis();
		CreateStatement cstmt = CreateTable(p1, p2, ExtVPFormat);
		cstmt.execute();
		
		SelectStatement mainstm = impala.select(String.format("t1.%s", column_name_subject));
		mainstm.addProjection(String.format("t1.%s", column_name_object));
		mainstm.from(String.format("(%s) t1", leftstmt));
		mainstm.leftSemiJoin(String.format("(%s) t2", rightstmt),
				String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_subject), false);

		impala.insertOverwrite(TableName_p1p2_OS).selectStatement(mainstm).execute();
		System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
		impala.computeStats(TableName_p1p2_OS);

		if (!isEmpty(TableName_p1p2_OS)) {
			double ExtVPSize = TableSize(TableName_p1p2_OS);
			double PartitionSize = TableSize(TT, p1);
			double Selectivity = ExtVPSize/PartitionSize;
			AddStats(TableName_p1p2_OS,p1, p2, ExtVPFormat, ExtVPSize, PartitionSize, Selectivity);
			if (Selectivity >= SF)
				impala.dropTable(TableName_p1p2_OS);
		} else {
			impala.dropTable(TableName_p1p2_OS);
			StoreEmptyTables(p1, p2, ExtVPFormat);
		}

		if (p1 != p2) {
			// CREATE TABLE ExtVP_p2_p1_os AS (SELECT t2.s, t2.o FROM (SELECT s,
			// o FROM TT WHERE p = p1) t1 RIGHT SEMI JOIN (SELECT s, o FROM TT
			// WHERE p = p2) t2 ON t1.s=t2.o);
			// COMPUTE STATS ExtVP_p2_p1_os;
			System.out.print(String.format("Creating %s from '%s'", TableName_p2p1_OS, TT));
			timestamp = System.currentTimeMillis();
			CreateStatement cstmt2 = CreateTable(p2, p1, ExtVPFormat);
			cstmt2.execute();

			SelectStatement mainstm2 = impala.select(String.format("t2.%s", column_name_subject));
			mainstm2.addProjection(String.format("t2.%s", column_name_object));
			mainstm2.from(String.format("(%s) t1", leftstmt));
			mainstm2.rightSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_object), false);

			impala.insertOverwrite(TableName_p2p1_OS).selectStatement(mainstm2).execute();
			System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
			impala.computeStats(TableName_p2p1_OS);

			if (!isEmpty(TableName_p2p1_OS)) {
				double ExtVPSize = TableSize(TableName_p2p1_OS);
				double PartitionSize = TableSize(TT, p2);
				double Selectivity = ExtVPSize/PartitionSize;
				AddStats(TableName_p2p1_OS,p2, p1, ExtVPFormat, ExtVPSize, PartitionSize, Selectivity);
				if (Selectivity >= SF)
					impala.dropTable(TableName_p2p1_OS);
			} else {
				impala.dropTable(TableName_p2p1_OS);
				StoreEmptyTables(p2, p1, ExtVPFormat);
			}
		}
	}

	private void Compute_SS(String TT, String p1, String p2, double SF, SelectStatement leftstmt, SelectStatement rightstmt, double PartitionSizeP1, double PartitionSizeP2) throws IllegalArgumentException, SQLException {

		String ExtVPFormat = "SS";
		String TableName_p1p2_SS = TableName(p1, p2, ExtVPFormat);
		String TableName_p2p1_SS = TableName(p2, p1, ExtVPFormat);
		// CREATE TABLE ExtVP_p1_p2_ss AS (SELECT t1.s, t1.o FROM (SELECT
		// s, o FROM TT WHERE p = p1) t1 LEFT SEMI JOIN (SELECT s, o FROM TT
		// WHERE p = p2) t2 ON t1.s=t2.s);
		// COMPUTE STATS ExtVP_p1_p2_ss;
		if (p1 != p2) {
			System.out.print(String.format("Creating %s from '%s'", TableName_p1p2_SS, TT));
			long timestamp = System.currentTimeMillis();
			CreateStatement cstmtSS = CreateTable(p1, p2, ExtVPFormat);
			cstmtSS.execute();
			
			SelectStatement mainstm = impala.select(String.format("t1.%s", column_name_subject));
			mainstm.addProjection(String.format("t1.%s", column_name_object));
			mainstm.from(String.format("(%s) t1", leftstmt));
			mainstm.leftSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_subject), false);

			impala.insertOverwrite(TableName_p1p2_SS).selectStatement(mainstm).execute();
			System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
			impala.computeStats(TableName_p1p2_SS);

			if (!isEmpty(TableName_p1p2_SS)) {
				// CREATE TABLE ExtVP_p2_p1_ss AS (SELECT t2.s, t2.o FROM
				// (SELECT s, o FROM TT WHERE p = p1) t1 RIGHT SEMI JOIN (SELECT
				// s, o FROM TT WHERE p = p2) t2 ON t1.s=t2.s);
				// COMPUTE STATS ExtVP_p2_p1_ss;
				System.out.print(String.format("Creating %s from '%s'", TableName_p2p1_SS, TT));
				timestamp = System.currentTimeMillis();
				CreateStatement cstmtSS2 = CreateTable(p2, p1, ExtVPFormat);
				cstmtSS2.execute();

				SelectStatement mainstm2 = impala.select(String.format("t2.%s", column_name_subject));
				mainstm2.addProjection(String.format("t2.%s", column_name_object));
				mainstm2.from(String.format("%s t1", TableName_p1p2_SS));
				mainstm2.rightSemiJoin(String.format("(%s) t2", rightstmt),
						String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_subject), false);

				impala.insertOverwrite(TableName_p2p1_SS).selectStatement(mainstm2).execute();
				System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
				impala.computeStats(TableName_p2p1_SS);
				
				double ExtVPSize = TableSize(TableName_p1p2_SS);
				double PartitionSize = TableSize(TT, p1);
				double Selectivity = ExtVPSize/PartitionSize;
				AddStats(TableName_p1p2_SS,p1, p2, ExtVPFormat, ExtVPSize, PartitionSize, Selectivity);
				if (Selectivity >= SF)
					impala.dropTable(TableName_p1p2_SS);
				
				ExtVPSize = TableSize(TableName_p2p1_SS);
				PartitionSize = TableSize(TT, p2);
				Selectivity = ExtVPSize/PartitionSize;
				AddStats(TableName_p2p1_SS,p2, p1, ExtVPFormat, ExtVPSize, PartitionSize, Selectivity);
				if (Selectivity >= SF)
					impala.dropTable(TableName_p2p1_SS);

			} else {
				impala.dropTable(TableName_p1p2_SS);
				StoreEmptyTables(p1, p2, ExtVPFormat);
				StoreEmptyTables(p2, p1, ExtVPFormat);
			}
		}
	}

	private void Compute_OO(String TT, String p1, String p2, double SF, SelectStatement leftstmt, SelectStatement rightstmt, double PartitionSizeP1, double PartitionSizeP2) throws IllegalArgumentException, SQLException {

		String ExtVPFormat = "OO";
		String TableName_p1p2_OO = TableName(p1, p2, ExtVPFormat);
		String TableName_p2p1_OO = TableName(p2, p1, ExtVPFormat);
		if (p1 != p2) {
			// CREATE TABLE ExtVP_p1_p2_oo AS (SELECT t1.s, t1.o FROM (SELECT s,
			// o
			// FROM TT WHERE p = p1) t1 LEFT SEMI JOIN (SELECT s, o FROM TT
			// WHERE p
			// = p2) t2 ON t1.o=t2.o);
			// COMPUTE STATS ExtVP_p1_p2_oo;

			System.out.print(String.format("Creating %s from '%s'", TableName_p1p2_OO, TT));
			long timestamp = System.currentTimeMillis();
			CreateStatement cstmtOO = CreateTable(p1, p2, ExtVPFormat);
			cstmtOO.execute();

			SelectStatement mainstm = impala.select(String.format("t1.%s", column_name_subject));
			mainstm.addProjection(String.format("t1.%s", column_name_object));
			mainstm.from(String.format("(%s) t1", leftstmt));
			mainstm.leftSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_object), false);

			impala.insertOverwrite(TableName_p1p2_OO).selectStatement(mainstm).execute();
			System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
			impala.computeStats(TableName_p1p2_OO);

			if (!isEmpty(TableName_p1p2_OO)) {
				// CREATE TABLE ExtVP_p2_p1_oo AS (SELECT t2.s, t2.o FROM
				// (SELECT s, o
				// FROM TT WHERE p = p1) t1 RIGHT SEMI JOIN (SELECT s, o FROM TT
				// WHERE p
				// = p2) t2 ON t1.o=t2.o);
				// COMPUTE STATS ExtVP_p2_p1_oo;
				System.out.print(String.format("Creating %s from '%s'", TableName_p2p1_OO, TT));
				timestamp = System.currentTimeMillis();
				CreateStatement cstmtOO2 = CreateTable(p2, p1, ExtVPFormat);
				cstmtOO2.execute();

				SelectStatement mainstm2 = impala.select(String.format("t2.%s", column_name_subject));
				mainstm2.addProjection(String.format("t2.%s", column_name_object));
				mainstm2.from(String.format("%s t1", TableName_p1p2_OO));
				mainstm2.rightSemiJoin(String.format("(%s) t2", rightstmt),
						String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_object), false);

				impala.insertOverwrite(TableName_p2p1_OO).selectStatement(mainstm2).execute();
				System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
				impala.computeStats(TableName_p2p1_OO);

				double ExtVPSize = TableSize(TableName_p1p2_OO);
				double PartitionSize = TableSize(TT, p1);
				double Selectivity = ExtVPSize/PartitionSize;
				AddStats(TableName_p1p2_OO,p1, p2, ExtVPFormat, ExtVPSize, PartitionSize, Selectivity);
				if (Selectivity >= SF)
					impala.dropTable(TableName_p1p2_OO);
				
				ExtVPSize = TableSize(TableName_p2p1_OO);
				PartitionSize = TableSize(TT, p2);
				Selectivity = ExtVPSize/PartitionSize;
				AddStats(TableName_p2p1_OO,p2, p1, ExtVPFormat, ExtVPSize, PartitionSize, Selectivity);
				if (Selectivity >= SF)
					impala.dropTable(TableName_p2p1_OO);

			} else {
				impala.dropTable(TableName_p1p2_OO);
				StoreEmptyTables(p1, p2, ExtVPFormat);
				StoreEmptyTables(p2, p1, ExtVPFormat);
			}
		}
	}

	private void Compute_SOandOS(String TT, String p1, String p2, double SF, SelectStatement leftstmt, SelectStatement rightstmt, double PartitionSizeP1, double PartitionSizeP2)
			throws IllegalArgumentException, SQLException {
		// CREATE TABLE ExtVP_p1_p2_so AS (SELECT t1.s, t1.o FROM (SELECT s, o
		// FROM TT WHERE p = p1) t1 LEFT SEMI JOIN (SELECT s, o FROM TT WHERE p
		// = p2) t2 ON t1.s=t2.o);
		// COMPUTE STATS ExtVP_p1_p2_so;
		String ExtVPFormatSO = "SO";
		String ExtVPFormatOS = "OS";
		String TableName_p1p2_SO = TableName(p1, p2, ExtVPFormatSO);
		String TableName_p2p1_OS = TableName(p2, p1, ExtVPFormatOS);
		String TableName_p1p2_OS = TableName(p1, p2, ExtVPFormatOS);
		String TableName_p2p1_SO = TableName(p2, p1, ExtVPFormatSO);
		System.out.print(String.format("Creating %s from '%s'", TableName_p1p2_SO, TT));
		long timestamp = System.currentTimeMillis();
		CreateStatement cstmtSO = CreateTable(p1, p2, ExtVPFormatSO);
		cstmtSO.execute();

		SelectStatement mainstm = impala.select(String.format("t1.%s", column_name_subject));
		mainstm.addProjection(String.format("t1.%s", column_name_object));
		mainstm.from(String.format("(%s) t1", leftstmt));
		mainstm.leftSemiJoin(String.format("(%s) t2", rightstmt),
				String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_object), false);

		impala.insertOverwrite(TableName_p1p2_SO).selectStatement(mainstm).execute();
		System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
		impala.computeStats(TableName_p1p2_SO);

		if (!isEmpty(TableName_p1p2_SO)) {
			// CREATE TABLE ExtVP_p2_p1_os AS (SELECT t2.s, t2.o FROM
			// ExtVP_p1_p2_so t1 RIGHT SEMI JOIN (SELECT s, o FROM TT WHERE p =
			// p2) t2 ON t1.s=t2.o);
			// COMPUTE STATS ExtVP_p2_p1_os;
			System.out.print(String.format("Creating %s from '%s'", TableName_p2p1_OS, TT));
			timestamp = System.currentTimeMillis();
			CreateStatement cstmtOS = CreateTable(p2, p1, ExtVPFormatOS);
			cstmtOS.execute();

			SelectStatement mainstm2 = impala.select(String.format("t2.%s", column_name_subject));
			mainstm2.addProjection(String.format("t2.%s", column_name_object));
			mainstm2.from(String.format("%s t1", TableName_p1p2_SO));
			mainstm2.rightSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_object), false);

			impala.insertOverwrite(TableName_p2p1_OS).selectStatement(mainstm2).execute();
			System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
			impala.computeStats(TableName_p2p1_OS);

			double ExtVPSize = TableSize(TableName_p1p2_SO);
			double PartitionSize = TableSize(TT, p1);
			double Selectivity = ExtVPSize/PartitionSize;
			AddStats(TableName_p1p2_SO, p1, p2, ExtVPFormatSO, ExtVPSize, PartitionSize, Selectivity);
			if (Selectivity >= SF)
				impala.dropTable(TableName_p1p2_SO);

			ExtVPSize = TableSize(TableName_p2p1_OS);
			PartitionSize = TableSize(TT, p2);
			Selectivity = ExtVPSize/PartitionSize;
			AddStats(TableName_p2p1_OS,p2, p1, ExtVPFormatOS, ExtVPSize, PartitionSize, Selectivity);
			if (Selectivity >= SF)
				impala.dropTable(TableName_p2p1_OS);

		} else {
			impala.dropTable(TableName_p1p2_SO);
			StoreEmptyTables(p1, p2, ExtVPFormatSO);
			StoreEmptyTables(p2, p1, ExtVPFormatOS);
		}
		if (p1 != p2) {
			// CREATE TABLE ExtVP_p1_p2_os AS (SELECT t1.s, t1.o FROM (SELECT s,
			// o
			// FROM TT WHERE p = p1) t1 LEFT SEMI JOIN (SELECT s, o FROM TT
			// WHERE p
			// = p2) t2 ON t1.o=t2.s);
			// COMPUTE STATS ExtVP_p1_p2_os;
			System.out.print(String.format("Creating %s from '%s'", TableName_p1p2_OS, TT));
			timestamp = System.currentTimeMillis();
			CreateStatement cstmtOS2 = CreateTable(p1, p2, ExtVPFormatOS);
			cstmtOS2.execute();

			SelectStatement mainstm3 = impala.select(String.format("t1.%s", column_name_subject));
			mainstm3.addProjection(String.format("t1.%s", column_name_object));
			mainstm3.from(String.format("(%s) t1", leftstmt));
			mainstm3.leftSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_subject), false);

			impala.insertOverwrite(TableName_p1p2_OS).selectStatement(mainstm3).execute();
			System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
			impala.computeStats(TableName_p1p2_OS);

			if (!isEmpty(TableName_p1p2_OS)) {
				// CREATE TABLE ExtVP_p2_p1_so AS (SELECT t2.s, t2.o FROM
				// ExtVP_p1_p2_os
				// t1 RIGHT SEMI JOIN (SELECT s, o FROM TT WHERE p = p2) t2 ON
				// t2.s=t1.o);
				// COMPUTE STATS ExtVP_p2_p1_os;
				System.out.print(String.format("Creating %s from '%s'", TableName_p2p1_SO, TT));
				timestamp = System.currentTimeMillis();
				CreateStatement cstmtSO2 = CreateTable(p2, p1, ExtVPFormatSO);
				cstmtSO2.execute();

				SelectStatement mainstm4 = impala.select(String.format("t2.%s", column_name_subject));
				mainstm4.addProjection(String.format("t2.%s", column_name_object));
				mainstm4.from(String.format("%s t1", TableName_p1p2_OS));
				mainstm4.rightSemiJoin(String.format("(%s) t2", rightstmt),
						String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_subject), false);

				impala.insertOverwrite(TableName_p2p1_SO).selectStatement(mainstm4).execute();
				System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
				impala.computeStats(TableName_p2p1_SO);

				double ExtVPSize = TableSize(TableName_p1p2_OS);
				double PartitionSize = TableSize(TT, p1);
				double Selectivity = ExtVPSize/PartitionSize;
				AddStats(TableName_p1p2_OS, p1, p2, ExtVPFormatOS, ExtVPSize, PartitionSize, Selectivity);
				if (TableSize(TableName_p1p2_OS) / TableSize(TT, p1) >= SF)
					impala.dropTable(TableName_p1p2_OS);

				ExtVPSize = TableSize(TableName_p2p1_SO);
				PartitionSize = TableSize(TT, p2);
				Selectivity = ExtVPSize/PartitionSize;
				AddStats(TableName_p2p1_SO,p2, p1, ExtVPFormatSO, ExtVPSize, PartitionSize, Selectivity);
				if (Selectivity >= SF)
					impala.dropTable(TableName_p2p1_SO);

			} else {
				impala.dropTable(TableName_p1p2_OS);
				StoreEmptyTables(p1, p2, ExtVPFormatOS);
				StoreEmptyTables(p2, p1, ExtVPFormatSO);
			}
		}
	}

	// GET TABLE STATISTICS, INFORMATION
	private boolean isEmpty(String Tablename) throws IllegalArgumentException, SQLException {
		ResultSet DataSet = impala.select().addProjection("Count(*) AS NrTuples").from(Tablename).execute();
		boolean Size = true;
		DataSet.next();
		if (Double.parseDouble(DataSet.getString("NrTuples")) != 0) {
			Size = false;
		} else {
			Size = true;
		}
		return Size;
	}

	private double TableSize(String Tablename, String Predicate) throws IllegalArgumentException, SQLException {
		ResultSet DataSet = impala.select().addProjection("Count(*) AS NrTuples").from(Tablename)
				.where(String.format("%s='%s'", column_name_predicate, Predicate)).execute();
		double Nrtuples = 0;
		DataSet.next();
		Nrtuples = Double.parseDouble(DataSet.getString("NrTuples"));
		return Nrtuples * 1.0000;
	}

	private double TableSize(String Tablename) throws IllegalArgumentException, SQLException {
		ResultSet DataSet = impala.select().addProjection("Count(*) AS NrTuples").from(Tablename).execute();
		double Nrtuples = 0;
		DataSet.next();
		Nrtuples = Double.parseDouble(DataSet.getString("NrTuples"));
		return Nrtuples * 1.000;
	}

	private SelectStatement SelectPartition(String TableName, String Predicate) {
		SelectStatement result = impala.select(column_name_subject);
		result.addProjection(column_name_object);
		result.from(String.format("%s", TableName));
		result.where(String.format("%s='%s'", column_name_predicate, Predicate));
		return result;
	}

	private CreateStatement CreateTable(String Predicate1, String Predicate2, String ExtVPFormat) {
		CreateStatement cstmt = impala.createTable(TableName(Predicate1, Predicate2, ExtVPFormat)).ifNotExists();
		cstmt.storedAs(FileFormat.PARQUET);
		cstmt.addColumnDefinition(column_name_subject, DataType.STRING);
		cstmt.addColumnDefinition(column_name_object, DataType.STRING);
		return cstmt;
	}

	private String TableName(String Predicate1, String Predicate2, String ExtVPFormat) {
		Predicate1 = RenamePredicates(Predicate1);
		Predicate2 = RenamePredicates(Predicate2);
		return String.format("%s_%s_%s_%s", tablename_output, Predicate1, Predicate2, ExtVPFormat);
	}

	private String RenamePredicates(String Predicate) {
		// NOT ALLOWED < > : // - / . , | # @
		String RenamedPredicate = Predicate.replaceAll("[<>/.,\\s\\-:\\?]", "_");
		return RenamedPredicate;
	}

	private void StoreEmptyTables(String Predicate1, String Predicate2, String ExtVPFormat)
			throws IllegalArgumentException, SQLException {

		try (FileWriter fw = new FileWriter("./EmptyTables.txt", true);
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter Append = new PrintWriter(bw)) {
			Append.println(TableName(Predicate1, Predicate2, ExtVPFormat));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void PhaseCompleted(int Pred1Possition, String Predicate1, int Pred2Possition, String Predicate2) throws IllegalArgumentException, SQLException {
		try (FileWriter fw = new FileWriter("./ExtVpCompleted.txt");
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter Append = new PrintWriter(bw)) {
			Append.println(String.format("%d,='%s',%d,='%s'", Pred1Possition, Predicate1, Pred2Possition, Predicate2));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private int GetCompletedPredicate(int PredicatOrder){
		int i=0;
		int result=0;
		if(PredicatOrder==2)
			i=2;
		try {
			BufferedReader br = new BufferedReader(new FileReader("./ExtVpCompleted.txt"));
			String line = br.readLine();
			String [] CompletedPredicates = line.toLowerCase().split("[,]");
			result = Integer.parseInt(CompletedPredicates[i]);	
			br.close();
		} catch (IOException e) {
			System.err.println("ExtVpCompleted.txt does not exist or it could not be opened. ExtVPs will start from beginning");
		}
		return result;
	}

	private void AddStats(String TableName, String p1, String p2, String ExtVPformat, double TuplesP1, double TuplesP2, double Selectivity){
		try (FileWriter fw = new FileWriter(String.format("./ExtVpStats_%s.txt", ExtVPformat), true);
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter Append = new PrintWriter(bw)) {
			Append.println(String.format("%s\t%s_%s\t%f\t%f\t%f",TableName, p1, p2, TuplesP1, TuplesP2, Selectivity));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
