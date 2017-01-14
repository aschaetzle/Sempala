package de.uni_freiburg.informatik.dbis.sempala.loader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import de.uni_freiburg.informatik.dbis.sempala.loader.sql.CreateStatement;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.Impala;
import de.uni_freiburg.informatik.dbis.sempala.loader.sql.InsertStatement;
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
	// Temporary for testing
	private String TT = "test";

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
		// buildTripleTable();

		// GetListOfPredicates given by user
		setListOfPredicates(TT);

		for (int i = 0; i < ListOfPredicates.size(); i++) {
			String p1 = ListOfPredicates.get(i);
			for (int j = i; j < ListOfPredicates.size(); j++) {
				String p2 = ListOfPredicates.get(j);
				if (!ExtVPTypes.isEmpty()) {
					if (ExtVPTypes.contains("so") && ExtVPTypes.contains("os")) {
						 Compute_SOandOS(TT, p1, p2, SF);
					} else if (ExtVPTypes.contains("so") && !ExtVPTypes.contains("os")) {
						 Compute_SO(TT, p1, p2, SF);
					} else if (!ExtVPTypes.contains("so") && ExtVPTypes.contains("os")) {
						 Compute_OS(TT, p1, p2, SF);
					} else {
					}
					if (ExtVPTypes.contains("ss")) {
						 Compute_SS(TT, p1, p2, SF);
					}
					if (ExtVPTypes.contains("oo")) {
						 Compute_OO(TT, p1, p2, SF);
					}
				} else {
					System.out.println("ExtVPTypes is empty");
				}
			}
		}
		System.out.println("Finished Terminate");
		System.exit(-1);
	}

	private void setThreshold(String threshold) {
		/**
		 * Ask for threshold in beginning
		 */
		try {
			SF = Double.parseDouble(threshold);
			if (SF <= 0)
				throw new IllegalArgumentException();
		} catch (Exception e) {
			System.out.print(String.format("Threshold '%s' is not a proper value as threshold", threshold));
			System.exit(1);
		}
		System.out.println(Double.toString(SF));
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
				System.out
						.print(String.format("'%s' is not a proper format of ExtVP types", SelectedTypes));
				System.exit(1);
			}
		} else {
			ExtVPTypes.add("ss");
			ExtVPTypes.add("so");
			ExtVPTypes.add("os");
			ExtVPTypes.add("oo");
		}
	}

	private void setListOfPredicates(String TripleTable) throws IllegalArgumentException, SQLException {
		ResultSet DataSet = impala.select(column_name_predicate).distinct().from(TripleTable).execute();
		while(DataSet.next()){
			ListOfPredicates.add(DataSet.getString(column_name_predicate));
		}
		for(int i=0; i<ListOfPredicates.size();i++){
			System.out.println(Double.toString(i+1) + " " + ListOfPredicates.get(i));
			
		}
	}

	// STILL NEED TO IMPROVE NAMING AND COMMENTS, WORKS FOR TEST(follows,likes)
	private void Compute_SO(String TT, String p1, String p2, double SF) throws IllegalArgumentException, SQLException {
		// CREATE TABLE ExtVP_p1_p2_so AS (SELECT t1.s, t1.o FROM (SELECT s, o
		// FROM TT WHERE p = p1) t1 LEFT SEMI JOIN (SELECT s, o FROM TT WHERE p
		// = p2) t2 ON t1.s=t2.o);
		// COMPUTE STATS ExtVP_p1_p2_so;
		String ExtVPFormat = "SO";

		System.out.print(String.format("Creating %s from '%s'", TableName(ExtVPFormat, p1, p2), TT));
		long timestamp = System.currentTimeMillis();

		CreateStatement cstmtSO = CreateTable(ExtVPFormat, p1, p2);
		cstmtSO.execute();

		SelectStatement leftstmt = SelectPartition(TT, p1);

		SelectStatement rightstmt = SelectPartition(TT, p2);

		SelectStatement mainstmt = impala.select(String.format("t1.%s", column_name_subject));
		mainstmt.addProjection(String.format("t1.%s", column_name_object));
		mainstmt.from(String.format("(%s) t1", leftstmt));
		mainstmt.leftSemiJoin(String.format("(%s) t2", rightstmt),
				String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_object), false);

		impala.insertOverwrite(TableName(p1, p2, ExtVPFormat)).selectStatement(mainstmt).execute();
		System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
		impala.computeStats(TableName(p1, p2, ExtVPFormat));

		// if((SELECT COUNT(*) FROM ExtVP_p1_p2_so / SELECT COUUNT(*) FROM TT
		// WHERE p=p1) >= SF)
		// DROP TABLE FROM ExtVP_p1_p2_so;
		if (!isEmpty(TableName(p1, p2, ExtVPFormat))) {
			System.out.println(Double.toString(TableSize(TableName(p1, p2, ExtVPFormat)) / TableSize(TT, p1)));
			if (TableSize(TableName(p1, p2, ExtVPFormat)) / TableSize(TT, p1) >= SF)
				impala.dropTable(TableName(p1, p2, ExtVPFormat));
		} else
			impala.dropTable(TableName(p1, p2, ExtVPFormat));

		if (p1 != p2) {
			// CREATE TABLE ExtVP_p2_p1_so AS (SELECT t2.s, t2.o FROM (SELECT s,
			// o
			// FROM TT WHERE p = p1) t1 RIGHT SEMI JOIN (SELECT s, o FROM TT
			// WHERE p
			// = p2) t2 ON t1.o=t2.s);
			// COMPUTE STATS ExtVP_p2_p1_so;
			System.out.print(String.format("Creating %s from '%s'", TableName(ExtVPFormat, p2, p1), TT));
			timestamp = System.currentTimeMillis();
			CreateStatement cstmtSO2 = CreateTable(ExtVPFormat, p2, p1);
			cstmtSO2.execute();

			SelectStatement mainstmt2 = impala.select(String.format("t2.%s", column_name_subject));
			mainstmt2.addProjection(String.format("t2.%s", column_name_object));
			mainstmt2.from(String.format("(%s) t1", leftstmt));
			mainstmt2.rightSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_subject), false);

			impala.insertOverwrite(TableName(p2, p1, ExtVPFormat)).selectStatement(mainstmt2).execute();
			System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
			impala.computeStats(TableName(p2, p1, ExtVPFormat));

			// if((SELECT COUNT(*) FROM ExtVP_p2_p1_so / SELECT COUUNT(*) FROM
			// TT
			// WHERE p=p2) >= SF)
			// DROP TABLE FROM ExtVP_p2_p1_so;
			if (!isEmpty(TableName(p2, p1, ExtVPFormat))) {
				System.out.println(Double.toString(TableSize(TableName(p2, p1, ExtVPFormat)) / TableSize(TT, p2)));
				if (TableSize(TableName(p2, p1, ExtVPFormat)) / TableSize(TT, p2) >= SF)
					impala.dropTable(TableName(p2, p1, ExtVPFormat));
			} else
				impala.dropTable(TableName(p2, p1, ExtVPFormat));
		}
	}

	private void Compute_OS(String TT, String p1, String p2, double SF) throws IllegalArgumentException, SQLException {
		// CREATE TABLE ExtVP_p1_p2_os AS (SELECT t1.s, t1.o FROM (SELECT s, o
		// FROM TT WHERE p = p1) t1 LEFT SEMI JOIN (SELECT s, o FROM TT WHERE p
		// = p2) t2 ON t1.o=t2.s);
		// COMPUTE STATS ExtVP_p1_p2_os;
		String ExtVPFormat = "OS";
		System.out.print(String.format("Creating %s from '%s'", TableName(ExtVPFormat, p1, p2), TT));
		long timestamp = System.currentTimeMillis();
		CreateStatement cstmt = CreateTable(ExtVPFormat, p1, p2);
		cstmt.execute();

		SelectStatement leftstmt = SelectPartition(TT, p1);

		SelectStatement rightstmt = SelectPartition(TT, p2);

		SelectStatement mainstm = impala.select(String.format("t1.%s", column_name_subject));
		mainstm.addProjection(String.format("t1.%s", column_name_object));
		mainstm.from(String.format("(%s) t1", leftstmt));
		mainstm.leftSemiJoin(String.format("(%s) t2", rightstmt),
				String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_subject), false);

		impala.insertOverwrite(TableName(p1, p2, ExtVPFormat)).selectStatement(mainstm).execute();
		System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
		impala.computeStats(TableName(p1, p2, ExtVPFormat));

		// if((SELECT COUNT(*) FROM ExtVP_p1_p2_os / SELECT COUUNT(*) FROM
		// TT WHERE p=p1) >= SF)
		// DROP TABLE FROM ExtVP_p1_p2_os;
		if (!isEmpty(TableName(p1, p2, ExtVPFormat))) {
			System.out.println(Double.toString(TableSize(TableName(p1, p2, ExtVPFormat)) / TableSize(TT, p1)));
			if (TableSize(TableName(p1, p2, ExtVPFormat)) / TableSize(TT, p1) >= SF)
				impala.dropTable(TableName(p1, p2, ExtVPFormat));
		} else
			impala.dropTable(TableName(p1, p2, ExtVPFormat));

		if (p1 != p2) {
			// CREATE TABLE ExtVP_p2_p1_os AS (SELECT t2.s, t2.o FROM (SELECT s,
			// o FROM TT WHERE p = p1) t1 RIGHT SEMI JOIN (SELECT s, o FROM TT
			// WHERE p = p2) t2 ON t1.s=t2.o);
			// COMPUTE STATS ExtVP_p2_p1_os;
			System.out.print(String.format("Creating %s from '%s'", TableName(ExtVPFormat, p2, p1), TT));
			timestamp = System.currentTimeMillis();
			CreateStatement cstmt2 = CreateTable(ExtVPFormat, p2, p1);
			cstmt2.execute();

			SelectStatement mainstm2 = impala.select(String.format("t2.%s", column_name_subject));
			mainstm2.addProjection(String.format("t2.%s", column_name_object));
			mainstm2.from(String.format("(%s) t1", leftstmt));
			mainstm2.rightSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_object), false);

			impala.insertOverwrite(TableName(p2, p1, ExtVPFormat)).selectStatement(mainstm2).execute();
			System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
			impala.computeStats(TableName(p2, p1, ExtVPFormat));

			// if((SELECT COUNT(*) FROM ExtVP_p2_p1_os / SELECT COUUNT(*) FROM
			// TT WHERE p=p2) >= SF)
			// DROP TABLE FROM ExtVP_p2_p1_os;
			if (!isEmpty(TableName(p2, p1, ExtVPFormat))) {
				System.out.println(Double.toString(TableSize(TableName(p2, p1, ExtVPFormat)) / TableSize(TT, p2)));
				if (TableSize(TableName(p2, p1, ExtVPFormat)) / TableSize(TT, p2) >= SF)
					impala.dropTable(TableName(p2, p1, ExtVPFormat));
			} else
				impala.dropTable(TableName(p2, p1, ExtVPFormat));
		}
	}

	private void Compute_SS(String TT, String p1, String p2, double SF) throws IllegalArgumentException, SQLException {

		String ExtVPFormat = "SS";
		// CREATE TABLE ExtVP_p1_p2_ss AS (SELECT t1.s, t1.o FROM (SELECT
		// s, o FROM TT WHERE p = p1) t1 LEFT SEMI JOIN (SELECT s, o FROM TT
		// WHERE p = p2) t2 ON t1.s=t2.s);
		// COMPUTE STATS ExtVP_p1_p2_ss;
		if (p1 != p2) {
			System.out.print(String.format("Creating %s from '%s'", TableName(ExtVPFormat, p1, p2), TT));
			long timestamp = System.currentTimeMillis();
			CreateStatement cstmtSS = CreateTable(ExtVPFormat, p1, p2);
			cstmtSS.execute();

			SelectStatement leftstmt = SelectPartition(TT, p1);

			SelectStatement rightstmt = SelectPartition(TT, p2);

			SelectStatement mainstm = impala.select(String.format("t1.%s", column_name_subject));
			mainstm.addProjection(String.format("t1.%s", column_name_object));
			mainstm.from(String.format("(%s) t1", leftstmt));
			mainstm.leftSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_subject), false);

			impala.insertOverwrite(TableName(p1, p2, ExtVPFormat)).selectStatement(mainstm).execute();
			System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
			impala.computeStats(TableName(p1, p2, ExtVPFormat));

			if (!isEmpty(TableName(p1, p2, ExtVPFormat))) {
				// CREATE TABLE ExtVP_p2_p1_ss AS (SELECT t2.s, t2.o FROM
				// (SELECT s, o FROM TT WHERE p = p1) t1 RIGHT SEMI JOIN (SELECT
				// s, o FROM TT WHERE p = p2) t2 ON t1.s=t2.s);
				// COMPUTE STATS ExtVP_p2_p1_ss;
				System.out.print(String.format("Creating %s from '%s'", TableName(ExtVPFormat, p2, p1), TT));
				timestamp = System.currentTimeMillis();
				CreateStatement cstmtSS2 = CreateTable(ExtVPFormat, p2, p1);
				cstmtSS2.execute();

				SelectStatement mainstm2 = impala.select(String.format("t2.%s", column_name_subject));
				mainstm2.addProjection(String.format("t2.%s", column_name_object));
				mainstm2.from(String.format("%s t1", TableName(p1, p2, ExtVPFormat)));
				mainstm2.rightSemiJoin(String.format("(%s) t2", rightstmt),
						String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_subject), false);

				impala.insertOverwrite(TableName(p2, p1, ExtVPFormat)).selectStatement(mainstm2).execute();
				System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
				impala.computeStats(TableName(p2, p1, ExtVPFormat));

				// if((SELECT COUNT(*) FROM ExtVP_p1_p2_ss / SELECT COUUNT(*)
				// FROM TT
				// WHERE p=p1) >= SF)
				// DROP TABLE FROM ExtVP_p1_p2_ss;
				System.out.print(Double.toString(TableSize(TableName(p1, p2, ExtVPFormat)) / TableSize(TT, p1)));
				if (TableSize(TableName(p1, p2, ExtVPFormat)) / TableSize(TT, p1) >= SF)
					impala.dropTable(TableName(p1, p2, ExtVPFormat));

				// if((SELECT COUNT(*) FROM ExtVP_p2_p1_ss / SELECT COUUNT(*)
				// FROM TT
				// WHERE p=p2) >= SF)
				// DROP TABLE FROM ExtVP_p2_p1_ss;
				System.out.print(Double.toString(TableSize(TableName(p2, p1, ExtVPFormat)) / TableSize(TT, p2)));
				if (TableSize(TableName(p2, p1, ExtVPFormat)) / TableSize(TT, p2) >= SF)
					impala.dropTable(TableName(p2, p1, ExtVPFormat));
			} else
				impala.dropTable(TableName(p1, p2, ExtVPFormat));
		}
	}

	private void Compute_OO(String TT, String p1, String p2, double SF) throws IllegalArgumentException, SQLException {

		String ExtVPFormat = "OO";
		if (p1 != p2) {
			// CREATE TABLE ExtVP_p1_p2_oo AS (SELECT t1.s, t1.o FROM (SELECT s,
			// o
			// FROM TT WHERE p = p1) t1 LEFT SEMI JOIN (SELECT s, o FROM TT
			// WHERE p
			// = p2) t2 ON t1.o=t2.o);
			// COMPUTE STATS ExtVP_p1_p2_oo;

			System.out.print(String.format("Creating %s from '%s'", TableName(ExtVPFormat, p1, p2), TT));
			long timestamp = System.currentTimeMillis();
			CreateStatement cstmtOO = CreateTable(ExtVPFormat, p1, p2);
			cstmtOO.execute();

			SelectStatement leftstmt = SelectPartition(TT, p1);

			SelectStatement rightstmt = SelectPartition(TT, p2);

			SelectStatement mainstm = impala.select(String.format("t1.%s", column_name_subject));
			mainstm.addProjection(String.format("t1.%s", column_name_object));
			mainstm.from(String.format("(%s) t1", leftstmt));
			mainstm.leftSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_object), false);

			impala.insertOverwrite(TableName(p1, p2, ExtVPFormat)).selectStatement(mainstm).execute();
			System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
			impala.computeStats(TableName(p1, p2, ExtVPFormat));

			if (!isEmpty(TableName(p1, p2, ExtVPFormat))) {
				// CREATE TABLE ExtVP_p2_p1_oo AS (SELECT t2.s, t2.o FROM
				// (SELECT s, o
				// FROM TT WHERE p = p1) t1 RIGHT SEMI JOIN (SELECT s, o FROM TT
				// WHERE p
				// = p2) t2 ON t1.o=t2.o);
				// COMPUTE STATS ExtVP_p2_p1_oo;
				System.out.print(String.format("Creating %s from '%s'", TableName(ExtVPFormat, p2, p1), TT));
				timestamp = System.currentTimeMillis();
				CreateStatement cstmtOO2 = CreateTable(ExtVPFormat, p2, p1);
				cstmtOO2.execute();

				SelectStatement mainstm2 = impala.select(String.format("t2.%s", column_name_subject));
				mainstm2.addProjection(String.format("t2.%s", column_name_object));
				mainstm2.from(String.format("%s t1", TableName(p1, p2, ExtVPFormat)));
				mainstm2.rightSemiJoin(String.format("(%s) t2", rightstmt),
						String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_object), false);

				impala.insertOverwrite(TableName(p2, p1, ExtVPFormat)).selectStatement(mainstm2).execute();
				System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
				impala.computeStats(TableName(p2, p1, ExtVPFormat));

				// if((SELECT COUNT(*) FROM ExtVP_p1_p2_oo / SELECT COUUNT(*)
				// FROM TT
				// WHERE p=p1) >= SF)
				// DROP TABLE FROM ExtVP_p1_p2_oo;
				System.out.println(Double.toString(TableSize(TableName(p1, p2, ExtVPFormat)) / TableSize(TT, p1)));
				if (TableSize(TableName(p1, p2, ExtVPFormat)) / TableSize(TT, p1) >= SF)
					impala.dropTable(TableName(p1, p2, ExtVPFormat));

				// if((SELECT COUNT(*) FROM ExtVP_p2_p1_oo / SELECT COUUNT(*)
				// FROM TT
				// WHERE p=p2) >= SF)
				// DROP TABLE FROM ExtVP_p2_p1_oo;
				System.out.println(Double.toString(TableSize(TableName(p2, p1, ExtVPFormat)) / TableSize(TT, p2)));
				if (TableSize(TableName(p2, p1, ExtVPFormat)) / TableSize(TT, p2) >= SF)
					impala.dropTable(TableName(p2, p1, ExtVPFormat));
			} else
				impala.dropTable(TableName(p1, p2, ExtVPFormat));
		}
	}

	private void Compute_SOandOS(String TT, String p1, String p2, double SF)
			throws IllegalArgumentException, SQLException {
		// CREATE TABLE ExtVP_p1_p2_so AS (SELECT t1.s, t1.o FROM (SELECT s, o
		// FROM TT WHERE p = p1) t1 LEFT SEMI JOIN (SELECT s, o FROM TT WHERE p
		// = p2) t2 ON t1.s=t2.o);
		// COMPUTE STATS ExtVP_p1_p2_so;
		String ExtVPFormatSO = "SO";
		String ExtVPFormatOS = "OS";

		// CreateStatement cstmt =
		// impala.createTable(String.format("ExtVP_%s_%s_SO", p1,
		// p2)).ifNotExists();
		// cstmt.storedAs(FileFormat.PARQUET);
		// cstmt.addColumnDefinition(column_name_subject, DataType.STRING);
		// cstmt.addColumnDefinition(column_name_object, DataType.STRING);
		// cstmt.execute();
		System.out.print(String.format("Creating %s from '%s'", TableName(ExtVPFormatSO, p1, p2), TT));
		long timestamp = System.currentTimeMillis();
		CreateStatement cstmtSO = CreateTable(ExtVPFormatSO, p1, p2);
		cstmtSO.execute();

		// SelectStatement leftstmt = impala.select(column_name_subject);
		// leftstmt.addProjection(column_name_object);
		// leftstmt.from(String.format("%s", TT));
		// leftstmt.where(String.format("p='%s'", p1));
		SelectStatement leftstmt = SelectPartition(TT, p1);

		// SelectStatement rightstmt = impala.select(column_name_subject);
		// rightstmt.addProjection(column_name_object);
		// rightstmt.from(String.format("%s", TT));
		// rightstmt.where(String.format("p='%s'", p2));
		SelectStatement rightstmt = SelectPartition(TT, p2);

		SelectStatement mainstm = impala.select(String.format("t1.%s", column_name_subject));
		mainstm.addProjection(String.format("t1.%s", column_name_object));
		mainstm.from(String.format("(%s) t1", leftstmt));
		mainstm.leftSemiJoin(String.format("(%s) t2", rightstmt),
				String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_object), false);

		impala.insertOverwrite(TableName(p1, p2, ExtVPFormatSO)).selectStatement(mainstm).execute();
		System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
		impala.computeStats(TableName(p1, p2, ExtVPFormatSO));

		// if(SELECT COUNT(*) FROM ExtVP_p1_p2_so != 0)
		if (!isEmpty(TableName(p1, p2, ExtVPFormatSO))) {
			// CREATE TABLE ExtVP_p2_p1_os AS (SELECT t2.s, t2.o FROM
			// ExtVP_p1_p2_so t1 RIGHT SEMI JOIN (SELECT s, o FROM TT WHERE p =
			// p2) t2 ON t1.s=t2.o);
			// COMPUTE STATS ExtVP_p2_p1_os;
			// CreateStatement cstmt2 =
			// impala.createTable(String.format("ExtVP_%s_%s_OS", p2,
			// p1)).ifNotExists();
			// cstmt2.storedAs(FileFormat.PARQUET);
			// cstmt2.addColumnDefinition(column_name_subject, DataType.STRING);
			// cstmt2.addColumnDefinition(column_name_object, DataType.STRING);
			// cstmt2.execute();
			System.out.print(String.format("Creating %s from '%s'", TableName(ExtVPFormatOS, p2, p1), TT));
			timestamp = System.currentTimeMillis();
			CreateStatement cstmtOS = CreateTable(ExtVPFormatOS, p2, p1);
			cstmtOS.execute();

			// SelectStatement leftstmtextvp =
			// impala.select(column_name_subject);
			// leftstmtextvp.addProjection(column_name_object);
			// leftstmtextvp.from(String.format("%s",
			// String.format("ExtVP_%s_%s_SO", p1, p2)));

			SelectStatement mainstm2 = impala.select(String.format("t2.%s", column_name_subject));
			mainstm2.addProjection(String.format("t2.%s", column_name_object));
			mainstm2.from(String.format("%s t1", TableName(p1, p2, ExtVPFormatSO)));
			mainstm2.rightSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_subject, "t2", column_name_object), false);

			impala.insertOverwrite(TableName(p2, p1, ExtVPFormatOS)).selectStatement(mainstm2).execute();
			System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
			impala.computeStats(TableName(p2, p1, ExtVPFormatOS));

			// if((SELECT COUNT(*) FROM ExtVP_p1_p2_so / SELECT COUUNT(*) FROM
			// TT WHERE p=p1) >= SF)
			// DROP TABLE FROM ExtVP_p1_p2_so;
			System.out.println(Double.toString(TableSize(TableName(p1, p2, ExtVPFormatSO)) / TableSize(TT, p1)));
			if (TableSize(TableName(p1, p2, ExtVPFormatSO)) / TableSize(TT, p1) >= SF)
				impala.dropTable(TableName(p1, p2, ExtVPFormatSO));
			// if((SELECT COUNT(*) FROM ExtVP_p2_p1_os / SELECT COUUNT(*) FROM
			// TT WHERE p=p2) >= SF)
			// DROP TABLE FROM ExtVP_p2_p1_os;
			System.out.println(Double.toString(TableSize(TableName(p2, p1, ExtVPFormatOS)) / TableSize(TT, p2)));
			if (TableSize(TableName(p2, p1, ExtVPFormatOS)) / TableSize(TT, p2) >= SF)
				impala.dropTable(TableName(p2, p1, ExtVPFormatOS));
		} else {
			// DROP TABLE FROM ExtVP_p1_p2_so;
			impala.dropTable(TableName(p1, p2, ExtVPFormatSO));
		}
		if (p1 != p2) {
			// CREATE TABLE ExtVP_p1_p2_os AS (SELECT t1.s, t1.o FROM (SELECT s,
			// o
			// FROM TT WHERE p = p1) t1 LEFT SEMI JOIN (SELECT s, o FROM TT
			// WHERE p
			// = p2) t2 ON t1.o=t2.s);
			// COMPUTE STATS ExtVP_p1_p2_os;
			// CreateStatement cstmt2 =
			// impala.createTable(String.format("ExtVP_%s_%s_OS", p1,
			// p2)).ifNotExists();
			// cstmt2.storedAs(FileFormat.PARQUET);
			// cstmt2.addColumnDefinition(column_name_subject, DataType.STRING);
			// cstmt2.addColumnDefinition(column_name_object, DataType.STRING);
			// cstmt2.execute();
			System.out.print(String.format("Creating %s from '%s'", TableName(ExtVPFormatOS, p1, p2), TT));
			timestamp = System.currentTimeMillis();
			CreateStatement cstmtOS2 = CreateTable(ExtVPFormatOS, p1, p2);
			cstmtOS2.execute();

			// SelectStatement leftstmt22 = impala.select(column_name_subject);
			// leftstmt22.addProjection(column_name_object);
			// leftstmt22.from(String.format("%s", TT));
			// leftstmt22.where(String.format("p='%s'", p1));

			// SelectStatement rightstmt22 = impala.select(column_name_subject);
			// rightstmt22.addProjection(column_name_object);
			// rightstmt22.from(String.format("%s", TT));
			// rightstmt22.where(String.format("p='%s'", p2));

			SelectStatement mainstm3 = impala.select(String.format("t1.%s", column_name_subject));
			mainstm3.addProjection(String.format("t1.%s", column_name_object));
			mainstm3.from(String.format("(%s) t1", leftstmt));
			mainstm3.leftSemiJoin(String.format("(%s) t2", rightstmt),
					String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_subject), false);

			impala.insertOverwrite(TableName(p1, p2, ExtVPFormatOS)).selectStatement(mainstm3).execute();
			System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
			impala.computeStats(TableName(p1, p2, ExtVPFormatOS));

			// if(SELECT COUNT(*) FROM ExtVP_p1_p2_os != 0)
			if (!isEmpty(TableName(p1, p2, ExtVPFormatOS))) {
				// CREATE TABLE ExtVP_p2_p1_so AS (SELECT t2.s, t2.o FROM
				// ExtVP_p1_p2_os
				// t1 RIGHT SEMI JOIN (SELECT s, o FROM TT WHERE p = p2) t2 ON
				// t2.s=t1.o);
				// COMPUTE STATS ExtVP_p2_p1_os;
				// CreateStatement cstmt22 =
				// impala.createTable(String.format("ExtVP_%s_%s_SO", p2,
				// p1)).ifNotExists();
				// cstmt22.storedAs(FileFormat.PARQUET);
				// cstmt22.addColumnDefinition(column_name_subject,
				// DataType.STRING);
				// cstmt22.addColumnDefinition(column_name_object,
				// DataType.STRING);
				// cstmt22.execute();
				System.out.print(String.format("Creating %s from '%s'", TableName(ExtVPFormatSO, p2, p1), TT));
				timestamp = System.currentTimeMillis();
				CreateStatement cstmtSO2 = CreateTable(ExtVPFormatSO, p2, p1);
				cstmtSO2.execute();

				// CHECK IF IT WORKS WITHOUT leftstmtextvp2 and JUST USE
				// String.format("ExtVP_%s_%s_OS", p1, p2)
				// SelectStatement leftstmtextvp2 =
				// impala.select(column_name_subject);
				// leftstmtextvp2.addProjection(column_name_object);
				// leftstmtextvp2.from(String.format("%s",
				// String.format("ExtVP_%s_%s_OS", p1, p2)));

				SelectStatement mainstm4 = impala.select(String.format("t2.%s", column_name_subject));
				mainstm4.addProjection(String.format("t2.%s", column_name_object));
				mainstm4.from(String.format("%s t1", TableName(p1, p2, ExtVPFormatOS)));
				mainstm4.rightSemiJoin(String.format("(%s) t2", rightstmt),
						String.format("%s.%s = %s.%s", "t1", column_name_object, "t2", column_name_subject), false);

				impala.insertOverwrite(TableName(p2, p1, ExtVPFormatSO)).selectStatement(mainstm4).execute();
				System.out.println(String.format(" [%.3fs]", (float) (System.currentTimeMillis() - timestamp) / 1000));
				impala.computeStats(TableName(p2, p1, ExtVPFormatSO));

				// if((SELECT COUNT(*) FROM ExtVP_p1_p2_os / SELECT COUUNT(*)
				// FROM TT
				// WHERE p=p1) >= SF)
				// DROP TABLE FROM ExtVP_p1_p2_os;
				System.out.println(Double.toString(TableSize(TableName(p1, p2, ExtVPFormatOS)) / TableSize(TT, p1)));
				if (TableSize(TableName(p1, p2, ExtVPFormatOS)) / TableSize(TT, p1) >= SF)
					impala.dropTable(TableName(p1, p2, ExtVPFormatOS));
				// if((SELECT COUNT(*) FROM ExtVP_p2_p1_so / SELECT COUUNT(*)
				// FROM TT
				// WHERE p=p2) >= SF)
				// DROP TABLE FROM ExtVP_p2_p1_so;
				System.out.println(Double.toString(TableSize(TableName(p2, p1, ExtVPFormatSO)) / TableSize(TT, p2)));
				if (TableSize(TableName(p2, p1, ExtVPFormatSO)) / TableSize(TT, p2) >= SF)
					impala.dropTable(TableName(p2, p1, ExtVPFormatSO));
			} else {
				// DROP TABLE FROM ExtVP_p2_p1_os;
				impala.dropTable(TableName(p1, p2, ExtVPFormatOS));
			}
		}
	}

	// GET TABLE STATISTICS, INFORMATION
	private boolean isEmpty(String Tablename) throws IllegalArgumentException, SQLException {
		ResultSet DataSet = impala.select().addProjection("Count(*) AS NrTuples").from(Tablename).execute();
		boolean TableName = true;
		DataSet.next();
		if (Double.parseDouble(DataSet.getString("NrTuples")) != 0) {
			TableName = false;
		} else {
			TableName = true;
		}
		return TableName;
	}

	private double TableSize(String Tablename, String Predicate) throws IllegalArgumentException, SQLException {
		ResultSet DataSet = impala.select().addProjection("Count(*) AS NrTuples").from(Tablename)
				.where(String.format("%s='%s'", column_name_predicate, Predicate)).execute();
		double Nrtuples = 0;
		DataSet.next();
		Nrtuples = Double.parseDouble(DataSet.getString("NrTuples"));
		return Nrtuples*1.0000;
	}

	private double TableSize(String Tablename) throws IllegalArgumentException, SQLException {
		ResultSet DataSet = impala.select().addProjection("Count(*) AS NrTuples").from(Tablename).execute();
		double Nrtuples = 0;
		DataSet.next();
		Nrtuples = Double.parseDouble(DataSet.getString("NrTuples"));
		return Nrtuples*1.000;
	}

	private SelectStatement SelectPartition(String TableName, String Predicate) {
		SelectStatement result = impala.select(column_name_subject);
		result.addProjection(column_name_object);
		result.from(String.format("%s", TableName));
		result.where(String.format("%s='%s'", column_name_predicate, Predicate));
		return result;
	}

	private CreateStatement CreateTable(String ExtVPFormat, String Predicate1, String Predicate2) {
		CreateStatement cstmt = impala.createTable(TableName(Predicate1, Predicate2, ExtVPFormat)).ifNotExists();
		cstmt.storedAs(FileFormat.PARQUET);
		cstmt.addColumnDefinition(column_name_subject, DataType.STRING);
		cstmt.addColumnDefinition(column_name_object, DataType.STRING);
		return cstmt;
	}

	private String TableName(String Predicate1, String Predicate2, String ExtVPFormat) {
		return String.format("%s_%s_%s_%s",tablename_output, Predicate1, Predicate2, ExtVPFormat);
	}

}
