package de.uni_freiburg.informatik.dbis.sempala.loader;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
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

/**
 * ExtVPLoader class creates the ExtVP tables from triples which are stored in initial Triple Table.
 * Computation of Triple Table is also executed in this class.
 * 
* @author Lis Bakalli <bakallil@informatik.uni-freiburg.de>
*
*/

public final class ExtVPLoader extends Loader {

	/** The constructor */
	public ExtVPLoader(Impala wrapper, String hdfsLocation) {
		super(wrapper, hdfsLocation);
		tablename_output = "extvp";
	}

	private ArrayList<String> ExtVPTypes = new ArrayList<String>();
	private ArrayList<String> ListOfPredicates = new ArrayList<>();
	
	//Default values for threshold, triple table and initial predicate
	private double SF = 1;
	private String TT = tablename_triple_table;
	private int FirstPredicate = 0;
	private int LastPredicate = 0;
	
	/**
	 * Creates Extended Vertical Partitioning tables from a triple table.
	 *
	 * The input table has to be in triple table format. The output will be a
	 * set of tables in format described in 'S2RDF: RDF Querying with
	 * SPARQL on Spark'.
	 *
	 * @throws SQLException
	 */
	@Override
	public void load() throws SQLException {
		// Get value of threshold
		setThreshold(threshold,EvaluationMode);

		// Specify ExtVP types to be calculated
		setExtVPTypes(extvp_types_selected);

		// Set the first predicate from which to start ExtVP calculation
		FirstPredicate = GetCompletedPredicate(1);
		
		// Load the triple table
		if (FirstPredicate == 0) {
			long timestampTT = System.currentTimeMillis();
			buildTripleTable();
			AddStats("BUILD TRIPLETABLE", " TIME", "", "Time", 0, 0,
					(double) (System.currentTimeMillis() - timestampTT) / 1000,0);
		}
		
		// Get list of predicates given by user
		setListOfPredicates(TT);
		
		//Get the last predicate if loading is partitioned
		LastPredicate = GetLastPredicate(Predicate_Partition);
		
		//Create ExtVP tables
		System.out.print(String.format("Creating %s from '%s' \n", "ExtVps", TT));
		long timestamptotal = System.currentTimeMillis();
		for (int i = FirstPredicate; i < LastPredicate; i++) {
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
		AddStats("Complete_EXTVP_TABLES",String.valueOf(FirstPredicate)+"-"+String.valueOf(LastPredicate),"","Time",0,0, (double) (System.currentTimeMillis() - timestamptotal) / 1000,0);
		
		//Store statistic files in HDFS
		try {
			StoreInHdfs(hdfs_input_directory);
		} catch (IllegalArgumentException | IOException e1) {
			e1.printStackTrace();
		}
		
		// Sleep 10 seconds to give impala some time to calm down
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		//Create tables of statistics
		System.out.print(String.format("Creating %s \n", "ExtVp Statistic Tables"));
		long timestampStats = System.currentTimeMillis();
		try {
			CreateStatsTables("SS",hdfs_input_directory,HdfsUserPath);
			CreateStatsTables("SO",hdfs_input_directory,HdfsUserPath);
			CreateStatsTables("OS",hdfs_input_directory,HdfsUserPath);
			CreateStatsTables("OO",hdfs_input_directory,HdfsUserPath);
			CreateStatsTables("TIME",hdfs_input_directory,HdfsUserPath);
			CreateStatsTables("EMPTY",hdfs_input_directory,HdfsUserPath);
		} catch (IllegalArgumentException | IOException e) {
			System.out.println("Stats tables could not be created. ");
			e.printStackTrace();
		}
		System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestampStats) / 1000));
		
		System.exit(-1);
	}

	/**
	 * Read value of threshold from CLI.
	 */
	private void setThreshold(String threshold, boolean evaluation_mode) {
		if (evaluation_mode)
			SF = 1.01;
		else {
			try {
				SF = Double.parseDouble(threshold);
				if (SF <= 0)
					throw new IllegalArgumentException();
			} catch (Exception e) {
				System.out.print(String.format("Threshold '%s' is not a proper value as threshold", threshold));
				System.exit(1);
			}
		}
	}

	/**
	 * Read types of ExtVP from CLI.
	 */
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

	/**
	 * Set the list of predicates for which the ExtVP tables will be calculated,
	 * if list of predicates is given, that list is used.
	 * 
	 * @param TripleTable - Table containing all triples.
	 * 
	 * @throws IllegalArgumentException
	 * @throws SQLException
	 */
	private void setListOfPredicates(String TripleTable) throws IllegalArgumentException, SQLException {
		if (path_of_list_of_predicates != "\\n") {
			System.out.println(String.format("Path for list of predicates is given: %s", path_of_list_of_predicates));
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

	/**
	 * Compute ExtVP table of type SO, OS, SS and OO for given predicates.
	 * 
	 * @param TT - Triple table.
	 * @param p1 - First predicate.
	 * @param p2 - Second predicate.
	 * @param SF - Selectivity threshold.
	 * @param leftstmt - Left statement of SemiJoin.
	 * @param rightstmt - Right statement of SemiJoin.
	 * @param PartitionSizeP1 - Partition size with predicate equal to first predicate.
	 * @param PartitionSizeP2 - Partition size with predicate equal to second predicate.
	 * 
	 * @throws IllegalArgumentException
	 * @throws SQLException
	 */
	private void Compute_SO(String TT, String p1, String p2, double SF, SelectStatement leftstmt, SelectStatement rightstmt, double PartitionSizeP1, double PartitionSizeP2) throws IllegalArgumentException, SQLException {
		String ExtVPFormat = "so";
		String TableName_p1p2_SO = TableName(p1, p2, ExtVPFormat);
		String TableName_p2p1_SO = TableName(p2, p1, ExtVPFormat);
		double Time = 0;
		
		System.out.print(String.format("Creating %s from '%s'", TableName_p1p2_SO, TT));
		long timestamp = System.currentTimeMillis();

		SelectStatement mainstmt = impala.select(String.format("t1.%s", column_name_subject));
		mainstmt.addProjection(String.format("t1.%s", column_name_object));
		mainstmt.from(String.format("(%s) t1", leftstmt));
		mainstmt.leftSemiJoin(String.format("(%s) t2", rightstmt),
				String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_object), false);
		
		CreateStatement cstmtSO = CreateTable(p1, p2, ExtVPFormat, mainstmt);
		cstmtSO.execute();
		Time = (float) (System.currentTimeMillis() - timestamp) / 1000;
		System.out.println(String.format(" [%.3fs]", Time));
		
		if (!isEmpty(TableName_p1p2_SO)) {
			impala.computeStats(TableName_p1p2_SO);
			double ExtVPSize = TableSize(TableName_p1p2_SO);
			double Selectivity = ExtVPSize/PartitionSizeP1;
			AddStats(TableName_p1p2_SO,p1, p2, ExtVPFormat, ExtVPSize, PartitionSizeP1, Selectivity, Time);
			if (Selectivity >= SF)
				impala.dropTable(TableName_p1p2_SO);
		} 
		else {
			impala.dropTable(TableName_p1p2_SO);
			StoreEmptyTables(TableName_p1p2_SO);
		}

		if (p1 != p2) {
			System.out.print(String.format("Creating %s from '%s'", TableName_p2p1_SO, TT));
			timestamp = System.currentTimeMillis();

			SelectStatement mainstmt2 = impala.select(String.format("t2.%s", column_name_subject));
			mainstmt2.addProjection(String.format("t2.%s", column_name_object));
			mainstmt2.from(String.format("(%s) t1", leftstmt));
			mainstmt2.rightSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_subject), false);
			
			CreateStatement cstmtSO2 = CreateTable(p2, p1, ExtVPFormat, mainstmt2);
			cstmtSO2.execute();
			Time = (float) (System.currentTimeMillis() - timestamp) / 1000;
			System.out.println(String.format(" [%.3fs]", Time));

			if (!isEmpty(TableName_p2p1_SO)) {
				impala.computeStats(TableName_p2p1_SO);
				double ExtVPSize = TableSize(TableName_p2p1_SO);
				double Selectivity = ExtVPSize/PartitionSizeP2;
				AddStats(TableName_p2p1_SO,p2, p1, ExtVPFormat, ExtVPSize, PartitionSizeP2, Selectivity, Time);
				if (TableSize(TableName_p2p1_SO) / TableSize(TT, p2) >= SF)
					impala.dropTable(TableName_p2p1_SO);
			} else {
				impala.dropTable(TableName_p2p1_SO);
				StoreEmptyTables(TableName_p2p1_SO);
			}
		}
	}
	
	private void Compute_OS(String TT, String p1, String p2, double SF, SelectStatement leftstmt, SelectStatement rightstmt, double PartitionSizeP1, double PartitionSizeP2) throws IllegalArgumentException, SQLException {
		String ExtVPFormat = "os";
		String TableName_p1p2_OS = TableName(p1, p2, ExtVPFormat);
		String TableName_p2p1_OS = TableName(p2, p1, ExtVPFormat);
		double Time = 0;
		System.out.print(String.format("Creating %s from '%s'", TableName_p1p2_OS, TT));
		long timestamp = System.currentTimeMillis();
		
		SelectStatement mainstm = impala.select(String.format("t1.%s", column_name_subject));
		mainstm.addProjection(String.format("t1.%s", column_name_object));
		mainstm.from(String.format("(%s) t1", leftstmt));
		mainstm.leftSemiJoin(String.format("(%s) t2", rightstmt),
				String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_subject), false);

		CreateStatement cstmt = CreateTable(p1, p2, ExtVPFormat, mainstm);
		cstmt.execute();
		Time = (float) (System.currentTimeMillis() - timestamp) / 1000;
		System.out.println(String.format(" [%.3fs]", Time));
		
		if (!isEmpty(TableName_p1p2_OS)) {
			impala.computeStats(TableName_p1p2_OS);
			double ExtVPSize = TableSize(TableName_p1p2_OS);
			double PartitionSize = TableSize(TT, p1);
			double Selectivity = ExtVPSize/PartitionSize;
			AddStats(TableName_p1p2_OS,p1, p2, ExtVPFormat, ExtVPSize, PartitionSize, Selectivity, Time);
			if (Selectivity >= SF)
				impala.dropTable(TableName_p1p2_OS);
		} else {
			impala.dropTable(TableName_p1p2_OS);
			StoreEmptyTables(TableName_p1p2_OS);
		}

		if (p1 != p2) {
			System.out.print(String.format("Creating %s from '%s'", TableName_p2p1_OS, TT));
			timestamp = System.currentTimeMillis();

			SelectStatement mainstm2 = impala.select(String.format("t2.%s", column_name_subject));
			mainstm2.addProjection(String.format("t2.%s", column_name_object));
			mainstm2.from(String.format("(%s) t1", leftstmt));
			mainstm2.rightSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_object), false);
			
			CreateStatement cstmt2 = CreateTable(p2, p1, ExtVPFormat, mainstm2);
			cstmt2.execute();
			Time = (float) (System.currentTimeMillis() - timestamp) / 1000;
			System.out.println(String.format(" [%.3fs]", Time));

			if (!isEmpty(TableName_p2p1_OS)) {
				impala.computeStats(TableName_p2p1_OS);
				double ExtVPSize = TableSize(TableName_p2p1_OS);
				double PartitionSize = TableSize(TT, p2);
				double Selectivity = ExtVPSize/PartitionSize;
				AddStats(TableName_p2p1_OS,p2, p1, ExtVPFormat, ExtVPSize, PartitionSize, Selectivity, Time);
				if (Selectivity >= SF)
					impala.dropTable(TableName_p2p1_OS);
			} else {
				impala.dropTable(TableName_p2p1_OS);
				StoreEmptyTables(TableName_p2p1_OS);
			}
		}
	}

	private void Compute_SS(String TT, String p1, String p2, double SF, SelectStatement leftstmt, SelectStatement rightstmt, double PartitionSizeP1, double PartitionSizeP2) throws IllegalArgumentException, SQLException {
		String ExtVPFormat = "ss";
		String TableName_p1p2_SS = TableName(p1, p2, ExtVPFormat);
		String TableName_p2p1_SS = TableName(p2, p1, ExtVPFormat);
		double Time = 0;
		double Time2 = 0;
		if (p1 != p2) {
			System.out.print(String.format("Creating %s from '%s'", TableName_p1p2_SS, TT));
			long timestamp = System.currentTimeMillis();
			
			SelectStatement mainstm = impala.select(String.format("t1.%s", column_name_subject));
			mainstm.addProjection(String.format("t1.%s", column_name_object));
			mainstm.from(String.format("(%s) t1", leftstmt));
			mainstm.leftSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_subject), false);

			CreateStatement cstmtSS = CreateTable(p1, p2, ExtVPFormat, mainstm);
			cstmtSS.execute();
			Time = (float) (System.currentTimeMillis() - timestamp) / 1000;
			System.out.println(String.format(" [%.3fs]", Time));
			
			if (!isEmpty(TableName_p1p2_SS)) {
				impala.computeStats(TableName_p1p2_SS);
				System.out.print(String.format("Creating %s from '%s'", TableName_p2p1_SS, TT));
				timestamp = System.currentTimeMillis();

				SelectStatement mainstm2 = impala.select(String.format("t2.%s", column_name_subject));
				mainstm2.addProjection(String.format("t2.%s", column_name_object));
				mainstm2.from(String.format("%s t1", TableName_p1p2_SS));
				mainstm2.rightSemiJoin(String.format("(%s) t2", rightstmt),
						String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_subject), false);

				CreateStatement cstmtSS2 = CreateTable(p2, p1, ExtVPFormat, mainstm2);
				cstmtSS2.execute();
				Time2 = (float) (System.currentTimeMillis() - timestamp) / 1000;
				System.out.println(String.format(" [%.3fs]", Time));
				impala.computeStats(TableName_p2p1_SS);
				
				double ExtVPSize = TableSize(TableName_p1p2_SS);
				double PartitionSize = TableSize(TT, p1);
				double Selectivity = ExtVPSize/PartitionSize;
				AddStats(TableName_p1p2_SS,p1, p2, ExtVPFormat, ExtVPSize, PartitionSize, Selectivity, Time);
				if (Selectivity >= SF)
					impala.dropTable(TableName_p1p2_SS);
				
				ExtVPSize = TableSize(TableName_p2p1_SS);
				PartitionSize = TableSize(TT, p2);
				Selectivity = ExtVPSize/PartitionSize;
				AddStats(TableName_p2p1_SS,p2, p1, ExtVPFormat, ExtVPSize, PartitionSize, Selectivity, Time2);
				if (Selectivity >= SF)
					impala.dropTable(TableName_p2p1_SS);

			} else {
				impala.dropTable(TableName_p1p2_SS);
				StoreEmptyTables(TableName_p1p2_SS);
				StoreEmptyTables(TableName_p2p1_SS);
			}
		}
	}

	private void Compute_OO(String TT, String p1, String p2, double SF, SelectStatement leftstmt, SelectStatement rightstmt, double PartitionSizeP1, double PartitionSizeP2) throws IllegalArgumentException, SQLException {
		String ExtVPFormat = "oo";
		String TableName_p1p2_OO = TableName(p1, p2, ExtVPFormat);
		String TableName_p2p1_OO = TableName(p2, p1, ExtVPFormat);
		double Time = 0;
		double Time2 = 0;
		if (p1 != p2) {
			System.out.print(String.format("Creating %s from '%s'", TableName_p1p2_OO, TT));
			long timestamp = System.currentTimeMillis();

			SelectStatement mainstm = impala.select(String.format("t1.%s", column_name_subject));
			mainstm.addProjection(String.format("t1.%s", column_name_object));
			mainstm.from(String.format("(%s) t1", leftstmt));
			mainstm.leftSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_object), false);

			CreateStatement cstmtOO = CreateTable(p1, p2, ExtVPFormat, mainstm);
			cstmtOO.execute();
			Time = (float) (System.currentTimeMillis() - timestamp) / 1000;
			System.out.println(String.format(" [%.3fs]", Time));
			
			if (!isEmpty(TableName_p1p2_OO)) {
				impala.computeStats(TableName_p1p2_OO);
				System.out.print(String.format("Creating %s from '%s'", TableName_p2p1_OO, TT));
				timestamp = System.currentTimeMillis();

				SelectStatement mainstm2 = impala.select(String.format("t2.%s", column_name_subject));
				mainstm2.addProjection(String.format("t2.%s", column_name_object));
				mainstm2.from(String.format("%s t1", TableName_p1p2_OO));
				mainstm2.rightSemiJoin(String.format("(%s) t2", rightstmt),
						String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_object), false);

				CreateStatement cstmtOO2 = CreateTable(p2, p1, ExtVPFormat, mainstm2);
				cstmtOO2.execute();
				Time2 = (float) (System.currentTimeMillis() - timestamp) / 1000;
				System.out.println(String.format(" [%.3fs]", Time2));
				impala.computeStats(TableName_p2p1_OO);

				double ExtVPSize = TableSize(TableName_p1p2_OO);
				double PartitionSize = TableSize(TT, p1);
				double Selectivity = ExtVPSize/PartitionSize;
				AddStats(TableName_p1p2_OO,p1, p2, ExtVPFormat, ExtVPSize, PartitionSize, Selectivity, Time);
				if (Selectivity >= SF)
					impala.dropTable(TableName_p1p2_OO);
				
				ExtVPSize = TableSize(TableName_p2p1_OO);
				PartitionSize = TableSize(TT, p2);
				Selectivity = ExtVPSize/PartitionSize;
				AddStats(TableName_p2p1_OO,p2, p1, ExtVPFormat, ExtVPSize, PartitionSize, Selectivity, Time2);
				if (Selectivity >= SF)
					impala.dropTable(TableName_p2p1_OO);

			} else {
				impala.dropTable(TableName_p1p2_OO);
				StoreEmptyTables(TableName_p1p2_OO);
				StoreEmptyTables(TableName_p2p1_OO);
			}
		}
	}

	private void Compute_SOandOS(String TT, String p1, String p2, double SF, SelectStatement leftstmt, SelectStatement rightstmt, double PartitionSizeP1, double PartitionSizeP2) throws IllegalArgumentException, SQLException {
		String ExtVPFormatSO = "so";
		String ExtVPFormatOS = "os";
		String TableName_p1p2_SO = TableName(p1, p2, ExtVPFormatSO);
		String TableName_p2p1_OS = TableName(p2, p1, ExtVPFormatOS);
		String TableName_p1p2_OS = TableName(p1, p2, ExtVPFormatOS);
		String TableName_p2p1_SO = TableName(p2, p1, ExtVPFormatSO);
		double Time = 0;
		double Time2 = 0;
		System.out.print(String.format("Creating %s from '%s'", TableName_p1p2_SO, TT));
		long timestamp = System.currentTimeMillis();		
		
		SelectStatement mainstm = impala.select(String.format("t1.%s", column_name_subject));
		mainstm.addProjection(String.format("t1.%s", column_name_object));
		mainstm.from(String.format("(%s) t1", leftstmt));
		mainstm.leftSemiJoin(String.format("(%s) t2", rightstmt),
				String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_object), false);
		
		CreateStatement cstmtSO = CreateTable(p1, p2, ExtVPFormatSO, mainstm);
		cstmtSO.execute();
		Time = (float) (System.currentTimeMillis() - timestamp) / 1000;
		System.out.println(String.format(" [%.3fs]", Time));
		

		if (!isEmpty(TableName_p1p2_SO)) {
			impala.computeStats(TableName_p1p2_SO);
			System.out.print(String.format("Creating %s from '%s'", TableName_p2p1_OS, TT));
			timestamp = System.currentTimeMillis();

			SelectStatement mainstm2 = impala.select(String.format("t2.%s", column_name_subject));
			mainstm2.addProjection(String.format("t2.%s", column_name_object));
			mainstm2.from(String.format("%s t1", TableName_p1p2_SO));
			mainstm2.rightSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_object), false);

			CreateStatement cstmtOS = CreateTable(p2, p1, ExtVPFormatOS, mainstm2);
			cstmtOS.execute();
			Time2 = (float) (System.currentTimeMillis() - timestamp) / 1000;
			System.out.println(String.format(" [%.3fs]", Time2));
			impala.computeStats(TableName_p2p1_OS);

			double ExtVPSize = TableSize(TableName_p1p2_SO);
			double PartitionSize = TableSize(TT, p1);
			double Selectivity = ExtVPSize/PartitionSize;
			AddStats(TableName_p1p2_SO, p1, p2, ExtVPFormatSO, ExtVPSize, PartitionSize, Selectivity, Time);
			if (Selectivity >= SF)
				impala.dropTable(TableName_p1p2_SO);

			ExtVPSize = TableSize(TableName_p2p1_OS);
			PartitionSize = TableSize(TT, p2);
			Selectivity = ExtVPSize/PartitionSize;
			AddStats(TableName_p2p1_OS,p2, p1, ExtVPFormatOS, ExtVPSize, PartitionSize, Selectivity, Time2);
			if (Selectivity >= SF)
				impala.dropTable(TableName_p2p1_OS);

		} else {
			impala.dropTable(TableName_p1p2_SO);
			StoreEmptyTables(TableName_p1p2_SO);
			StoreEmptyTables(TableName_p2p1_OS);
		}
		if (p1 != p2) {
			System.out.print(String.format("Creating %s from '%s'", TableName_p1p2_OS, TT));
			timestamp = System.currentTimeMillis();

			SelectStatement mainstm3 = impala.select(String.format("t1.%s", column_name_subject));
			mainstm3.addProjection(String.format("t1.%s", column_name_object));
			mainstm3.from(String.format("(%s) t1", leftstmt));
			mainstm3.leftSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_subject), false);
			
			CreateStatement cstmtOS2 = CreateTable(p1, p2, ExtVPFormatOS, mainstm3);
			cstmtOS2.execute();
			Time = (float) (System.currentTimeMillis() - timestamp) / 1000;
			System.out.println(String.format(" [%.3fs]", Time));


			if (!isEmpty(TableName_p1p2_OS)) {
				impala.computeStats(TableName_p1p2_OS);
				System.out.print(String.format("Creating %s from '%s'", TableName_p2p1_SO, TT));
				timestamp = System.currentTimeMillis();


				SelectStatement mainstm4 = impala.select(String.format("t2.%s", column_name_subject));
				mainstm4.addProjection(String.format("t2.%s", column_name_object));
				mainstm4.from(String.format("%s t1", TableName_p1p2_OS));
				mainstm4.rightSemiJoin(String.format("(%s) t2", rightstmt),
						String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_subject), false);

				CreateStatement cstmtSO2 = CreateTable(p2, p1, ExtVPFormatSO, mainstm4);
				cstmtSO2.execute();
				Time2 = (float) (System.currentTimeMillis() - timestamp) / 1000;
				System.out.println(String.format(" [%.3fs]", Time2));
				impala.computeStats(TableName_p2p1_SO);

				double ExtVPSize = TableSize(TableName_p1p2_OS);
				double PartitionSize = TableSize(TT, p1);
				double Selectivity = ExtVPSize/PartitionSize;
				AddStats(TableName_p1p2_OS, p1, p2, ExtVPFormatOS, ExtVPSize, PartitionSize, Selectivity, Time);
				if (TableSize(TableName_p1p2_OS) / TableSize(TT, p1) >= SF)
					impala.dropTable(TableName_p1p2_OS);

				ExtVPSize = TableSize(TableName_p2p1_SO);
				PartitionSize = TableSize(TT, p2);
				Selectivity = ExtVPSize/PartitionSize;
				AddStats(TableName_p2p1_SO,p2, p1, ExtVPFormatSO, ExtVPSize, PartitionSize, Selectivity, Time2);
				if (Selectivity >= SF)
					impala.dropTable(TableName_p2p1_SO);

			} else {
				impala.dropTable(TableName_p1p2_OS);
				StoreEmptyTables(TableName_p1p2_OS);
				StoreEmptyTables(TableName_p2p1_SO);
			}
		}
	}

	/**
	 * Check if a given table is empty.
	 * 
	 * @param Tablename - Name of the table which is checked
	 * @return true if table is empty.
	 * 
	 * @throws IllegalArgumentException
	 * @throws SQLException
	 */
	private boolean isEmpty(String Tablename) throws IllegalArgumentException, SQLException {
		ResultSet DataSet = impala.select().addProjection("Count(*) AS NrTuples").from(Tablename).execute();
		boolean Empty = true;
		DataSet.next();
		if (Double.parseDouble(DataSet.getString("NrTuples")) != 0) {
			Empty = false;
		} else {
			Empty = true;
		}
		return Empty;
	}

	/**
	 * Get the size of a given table, or the size of a partition inside the given table based on 
	 * predicate.
	 * 
	 * @param Tablename - Name of the table.
	 * @param Predicate - Specified predicate.
	 * @return table size.
	 * 
	 * @throws IllegalArgumentException
	 * @throws SQLException
	 */
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

	/**
	 * Select a specified partition inside a table based on predicate.
	 * 
	 * @param TableName - Name of the table
	 * @param Predicate - Specified predicate
	 * @return Select statement for the partition.
	 */
	private SelectStatement SelectPartition(String TableName, String Predicate) {
		SelectStatement result = impala.select(column_name_subject);
		result.addProjection(column_name_object);
		result.from(String.format("%s", TableName));
		result.where(String.format("%s='%s'", column_name_predicate, Predicate));
		return result;
	}
	
	/**
	 * Create empty ExtVP table based on predicates and format, without data.
	 * 
	 * @param Predicate1 - First predicate.
	 * @param Predicate2 - Second predicate.
	 * @param ExtVPFormat - ExtVP Format.
	 * @return Create statement for the ExtVP table.
	 */
	private CreateStatement CreateTable(String Predicate1, String Predicate2, String ExtVPFormat, SelectStatement stmt) {
		CreateStatement cstmt = impala.createTable(TableName(Predicate1, Predicate2, ExtVPFormat)).ifNotExists();
		cstmt.storedAs(FileFormat.PARQUET);
		cstmt.asSelect(stmt);
		return cstmt;
	}

	/**
	 * Set the name of the ExtVP table based on predicates and format.
	 * 
	 * @param Predicate1 - First predicate.
	 * @param Predicate2 - Second predicate.
	 * @param ExtVPFormat - ExtVP Format.
	 * @return Name of the table.
	 */
	private String TableName(String Predicate1, String Predicate2, String ExtVPFormat) {
		Predicate1 = RenamePredicates(Predicate1);
		Predicate2 = RenamePredicates(Predicate2);
		return String.format("%s_%s_%s_%s", tablename_output, Predicate1, Predicate2, ExtVPFormat);
	}
	
	/**
	 * Rename the predicate by replacing restricted characters for table names.
	 * 
	 * @param Predicate - Predicate to be modified.
	 * 
	 * @return New predicate with replaced characters.
	 */
	private String RenamePredicates(String Predicate) {
		// NOT ALLOWED < > : // - / . , | # @ ` ~
		String RenamedPredicate = Predicate.replaceAll("[<>/.`~#,\\s\\-:\\?]", "_");
		return RenamedPredicate;
	}

	/**
	 * Store empty ExtVP tables in a .txt file.
	 * 
	 * @param EmptyTable
	 * 
	 * @throws IllegalArgumentException
	 * @throws SQLException
	 */
	private void StoreEmptyTables(String EmptyTable)
			throws IllegalArgumentException, SQLException {

		try (FileWriter fw = new FileWriter("./EmptyTables_"+String.valueOf(FirstPredicate)+".txt", true);
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter Append = new PrintWriter(bw)) {
			Append.println(EmptyTable);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Store the predicate up to which the ExtVP tables are completed.
	 * 
	 * @param Pred1Possition - Position of first predicate of ExtVP table in the list of predicates.
	 * @param Predicate1 - First predicate.
	 * @param Pred2Possition - Position of second predicate of ExtVP table in the list of predicates.
	 * @param Predicate2 - Second predicate.
	 * 
	 * @throws IllegalArgumentException
	 * @throws SQLException
	 */
	private void PhaseCompleted(int Pred1Possition, String Predicate1, int Pred2Possition, String Predicate2) throws IllegalArgumentException, SQLException {
		try (FileWriter fw = new FileWriter("./ExtVpCompleted.txt");
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter Append = new PrintWriter(bw)) {
			Append.println(String.format("%d,='%s',%d,='%s'", Pred1Possition, Predicate1, Pred2Possition, Predicate2));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * In case loader for ExtVP is executed in several executions, 
	 * then get the last predicate for which the ExtVP tables have been calculated
	 * in previous execution. 
	 * @param PredicatOrder
	 * @return
	 */
	private int GetCompletedPredicate(int PredicatOrder){
		if (Predicate_Partition != "All") {
			String[] Position = Predicate_Partition.split(",");
			return Integer.parseInt(Position[0]);
		} 
		else {
			int i = 0;
			int result = 0;
			if (PredicatOrder == 2)
				i = 2;
			try {
				BufferedReader br = new BufferedReader(new FileReader("./ExtVpCompleted.txt"));
				String line = br.readLine();
				String[] CompletedPredicates = line.toLowerCase().split("[,]");
				result = Integer.parseInt(CompletedPredicates[i]);
				br.close();
			} catch (IOException e) {
				System.err.println(
						"ExtVpCompleted.txt does not exist or it could not be opened. ExtVPs will start from beginning");
			}
			return result;
		}
	}
	
	/**
	 * In case loader for ExtVP is executed in separated mode, 
	 * then get the last predicate for which the ExtVP tables have been to be calculated. 
	 * @param PredicatePartition
	 * @return
	 */
	private int GetLastPredicate(String PredicatePartition){
		if (PredicatePartition != "All") {
			String[] Position = Predicate_Partition.split(",");
			try {
				return Integer.parseInt(Position[1]);
			} catch (Exception e) {
				return ListOfPredicates.size();
			}
			
		}
		else
			return ListOfPredicates.size();
	}
	
	/**
	 * Store statistics of ExtVP table in .txt files.
	 * 
	 * @param TableName - Name of ExtVP table.
	 * @param p1 - First predicate.
	 * @param p2 - Second predicate.
	 * @param ExtVPformat - ExtVP format.
	 * @param ExtVPSize - ExtVP table size.
	 * @param VPSize - Partition size based on first predicate.
	 * @param Selectivity - Selectivity of ExtVP table size compared to partition size.
	 */
	private void AddStats(String TableName, String p1, String p2, String ExtVPformat, double ExtVPSize, double VPSize, double Selectivity, double Time){
		try (FileWriter fw = new FileWriter(String.format("./ExtVpStats_%s_%d.txt", ExtVPformat, FirstPredicate), true);
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter Append = new PrintWriter(bw)) {
			Append.println(String.format("%s\t%s_%s\t%f\t%f\t%f\t%f",TableName, p1, p2, ExtVPSize, VPSize, Selectivity, Time));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Put the statistic files into hdfs directories
	 * 
	 * @param DataSetName - Name of Dataset used to create the directory in Stats with the same name
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void StoreInHdfs(String DataSetName) throws IllegalArgumentException, IOException{	
		int index = DataSetName.lastIndexOf("/");
		String HdfsFolderName = DataSetName.substring(index);
		
		Runtime rt; 

		try {
			Thread.sleep(10000);
			rt = Runtime.getRuntime();
			rt.exec("hdfs dfs -mkdir ./Stats");
			Thread.sleep(2000);
			rt = Runtime.getRuntime();
			rt.exec("hdfs dfs -mkdir ./Stats" + HdfsFolderName);
			Thread.sleep(2000);
			rt = Runtime.getRuntime();
			rt.exec("hdfs dfs -mkdir ./Stats" + HdfsFolderName + "/Empty");
			Thread.sleep(2000);
			rt = Runtime.getRuntime();
			rt.exec("hdfs dfs -mkdir ./Stats" + HdfsFolderName + "/Time");
			Thread.sleep(2000);
			rt = Runtime.getRuntime();
			rt.exec("hdfs dfs -mkdir ./Stats" + HdfsFolderName + "/SS");
			Thread.sleep(2000);
			rt = Runtime.getRuntime();
			rt.exec("hdfs dfs -mkdir ./Stats" + HdfsFolderName + "/SO");
			Thread.sleep(2000);
			rt = Runtime.getRuntime();
			rt.exec("hdfs dfs -mkdir ./Stats" + HdfsFolderName + "/OS");
			Thread.sleep(2000);
			rt = Runtime.getRuntime();
			rt.exec("hdfs dfs -mkdir ./Stats" + HdfsFolderName + "/OO");
			Thread.sleep(10000);

			rt.exec("hadoop fs -chmod 777 ./Stats" + HdfsFolderName + "/OO");
			rt.exec("hadoop fs -chmod 777 ./Stats" + HdfsFolderName + "/OS");
			rt.exec("hadoop fs -chmod 777 ./Stats" + HdfsFolderName + "/SO");
			rt.exec("hadoop fs -chmod 777 ./Stats" + HdfsFolderName + "/SS");
			rt.exec("hadoop fs -chmod 777 ./Stats" + HdfsFolderName + "/Time");
			rt.exec("hadoop fs -chmod 777 ./Stats" + HdfsFolderName + "/Empty");
			rt.exec("hadoop fs -chmod 777 ./Stats" + HdfsFolderName);
			rt.exec("hadoop fs -chmod 777 ./Stats");

			rt.exec("hdfs dfs -put ./EmptyTables_"+String.valueOf(FirstPredicate)+".txt ./Stats" + HdfsFolderName + "/Empty");
			rt.exec("hdfs dfs -put ./ExtVpStats_Time_"+String.valueOf(FirstPredicate)+".txt ./Stats" + HdfsFolderName + "/Time");
			rt.exec("hdfs dfs -put ./ExtVpStats_ss_"+String.valueOf(FirstPredicate)+".txt ./Stats" + HdfsFolderName + "/SS");
			rt.exec("hdfs dfs -put ./ExtVpStats_so_"+String.valueOf(FirstPredicate)+".txt ./Stats" + HdfsFolderName + "/SO");
			rt.exec("hdfs dfs -put ./ExtVpStats_os_"+String.valueOf(FirstPredicate)+".txt ./Stats" + HdfsFolderName + "/OS");
			rt.exec("hdfs dfs -put ./ExtVpStats_oo_"+String.valueOf(FirstPredicate)+".txt ./Stats" + HdfsFolderName + "/OO");
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
		
	/**
	 * Store the ExtVP Loader phase statistics and ExtVP table statistics as a table.
	 * 
	 * @param ExtVPType - Type of statistic (SO, OS, SS, OO, TIME, EMPTY).
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws SQLException
	 */
	private void CreateStatsTables(String ExtVPType, String DataSetName, String UsersDirectory) throws FileNotFoundException, IOException, IllegalArgumentException, SQLException{
		int index = DataSetName.lastIndexOf("/");
		String HdfsFolderName = DataSetName.substring(index);	

		if(ExtVPType=="TIME"){
			impala.createTable("external_extvp_tableofstats_time").ifNotExists()
			.external()
			.addColumnDefinition("Operation_Name", DataType.STRING)
			.addColumnDefinition("Description", DataType.STRING)
			.addColumnDefinition("Time", DataType.DOUBLE)
			.addColumnDefinition("Measured", DataType.DOUBLE)
			.addColumnDefinition("Seconds", DataType.DOUBLE)
			.fieldTermintor(field_terminator)
			.lineTermintor(line_terminator)
			.location(UsersDirectory+"/Stats"+HdfsFolderName+"/Time/")
			.execute();
			
			impala.createTable("extvp_tableofstats_time").ifNotExists()
			.storedAs(FileFormat.PARQUET)
			.addColumnDefinition("Operation_Name", DataType.STRING)
			.addColumnDefinition("Description", DataType.STRING)
			.addColumnDefinition("Time", DataType.DOUBLE)
			.addColumnDefinition("Measured", DataType.DOUBLE)
			.addColumnDefinition("Seconds", DataType.DOUBLE)
			.execute();
			
			impala
			.insertOverwrite("extvp_tableofstats_time")
			.selectStatement(impala.select("Operation_Name")
			.addProjection("Description")
			.addProjection("Time") 
			.addProjection("Measured") 
			.addProjection("Seconds") 
			.from("external_extvp_tableofstats_time"))
			.execute();
			
			impala.computeStats("extvp_tableofstats_time");
			
			impala.dropTable("external_extvp_tableofstats_time");
		}
		else if(ExtVPType=="EMPTY"){
			impala.createTable("external_extvp_tableofstats_emptytable").ifNotExists()
			.external()
			.addColumnDefinition("ExtVPTable_Name", DataType.STRING)
			.lineTermintor(line_terminator)
			.location(UsersDirectory+"/Stats"+HdfsFolderName+"/Empty/")
			.execute();
			
			impala.createTable("extvp_tableofstats_emptytable").ifNotExists()
			.storedAs(FileFormat.PARQUET)
			.addColumnDefinition("ExtVPTable_Name", DataType.STRING)
			.execute();
			
			impala
			.insertOverwrite("extvp_tableofstats_emptytable")
			.selectStatement(impala.select("ExtVPTable_Name")
			.from("external_extvp_tableofstats_emptytable"))
			.execute();
			
			impala.computeStats("extvp_tableofstats_emptytable");
			
			impala.dropTable("external_extvp_tableofstats_emptytable");
		}
		else{
			impala.createTable("external_extvp_tableofstats_"+ExtVPType).ifNotExists()
			.external()
			.addColumnDefinition("ExtVPTable_Name", DataType.STRING)
			.addColumnDefinition("ExtVPTable_Predicates", DataType.STRING)
			.addColumnDefinition("ExtVPTable_Nr_Tuples", DataType.DOUBLE)
			.addColumnDefinition("Partition_Nr_Tuples", DataType.DOUBLE)
			.addColumnDefinition("ExtVPTable_SF", DataType.DOUBLE)
			.addColumnDefinition("ExtVPTable_Time", DataType.DOUBLE)
			.fieldTermintor(field_terminator)
			.lineTermintor(line_terminator)
			.location(UsersDirectory+"/Stats"+HdfsFolderName+"/"+ExtVPType+"/")
			.execute();
			
			impala.createTable("extvp_tableofstats_"+ExtVPType)
			.ifNotExists()
			.storedAs(FileFormat.PARQUET)
			.addColumnDefinition("ExtVPTable_Name", DataType.STRING)
			.addColumnDefinition("ExtVPTable_Predicates", DataType.STRING)
			.addColumnDefinition("ExtVPTable_Nr_Tuples", DataType.DOUBLE)
			.addColumnDefinition("Partition_Nr_Tuples", DataType.DOUBLE)
			.addColumnDefinition("ExtVPTable_SF", DataType.DOUBLE)
			.addColumnDefinition("ExtVPTable_Time", DataType.DOUBLE)
			.execute();
			
			impala
			.insertOverwrite("extvp_tableofstats_"+ExtVPType)
			.selectStatement(impala.select("ExtVPTable_Name")
			.addProjection("ExtVPTable_Predicates")
			.addProjection("ExtVPTable_Nr_Tuples") 
			.addProjection("Partition_Nr_Tuples") 
			.addProjection("ExtVPTable_SF") 
			.addProjection("ExtVPTable_Time")
			.from("external_extvp_tableofstats_"+ExtVPType))
			.execute();
			
			impala.computeStats("extvp_tableofstats_"+ExtVPType);
			
			impala.dropTable("external_extvp_tableofstats_"+ExtVPType);
		}
	}
}
