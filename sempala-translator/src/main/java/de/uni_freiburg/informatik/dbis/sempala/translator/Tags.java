package de.uni_freiburg.informatik.dbis.sempala.translator;

import java.util.HashMap;

/**
 *
 * @author ALKA2008
 */
public final class Tags {

	// Global Constants
	public static final String IMPALA_PROPERTYTABLE_TABLENAME = "bigtable_parquet";
	public static final String IMPALA_SINGLETABLE_TABLENAME = "singletable";
	public static final String IMPALA_TABLENAME_TRIPLESTORE = "triplestore_parquet";

	// tables produced by the complex property table loading process
	public static final String COMPLEX_TRIPLETABLE_TABLENAME = "tripletable";
	public static final String COMPLEX_PROPERTYTABLE_TABLENAME = "complex_property_table";
	// spark caches the complex_property_table under this table
	public static final String CACHED_COMPLEX_PROPERTYTABLE_TABLENAME = "cached_complex_property_table";
	public static final String COMPLEX_PROPERTIES_TABLENAME = "properties";
	
	//tables produced by the ExtVP loading process
	public static final String TABLENAME_TRIPLE_TABLE = "tripletable";
	
	public static final String SEMPALA_RESULTS_DB_NAME = "sempala_results";

	public static final String BGP = "BGP";
	public static final String FILTER = "FILTER";
	public static final String JOIN = "JOIN";
	public static final String SEQUENCE = "SEQUENCE_JOIN";
	public static final String LEFT_JOIN = "OPTIONAL";
	public static final String CONDITIONAL = "OPTIONAL";
	public static final String UNION = "UNION";
	public static final String PROJECT = "PROJECTION ";
	public static final String DISTINCT = "SM_Distinct";
	public static final String ORDER = "SM_Order";
	public static final String SLICE = "SM_Slice";
	public static final String REDUCED = "SM_Reduced";

	public static final String GREATER_THAN = " > ";
	public static final String GREATER_THAN_OR_EQUAL = " >= ";
	public static final String LESS_THAN = " < ";
	public static final String LESS_THAN_OR_EQUAL = " <= ";
	public static final String EQUALS = " = ";
	public static final String NOT_EQUALS = " != ";
	public static final String LOGICAL_AND = " AND ";
	public static final String LOGICAL_OR = " OR ";
	public static final String LOGICAL_NOT = "NOT ";
	public static final String BOUND = " IS NOT NULL";
	public static final String NOT_BOUND = " IS NULL";

	public static final String NO_VAR = "#noVar";
	public static final String NO_SUPPORT = "#noSupport";

	public static final String OFFSETCHAR = "\t";
	public static final String SUBJECT_COLUMN_NAME   = "s";
	public static final String PREDICATE_COLUMN_NAME = "p";
	public static final String OBJECT_COLUMN_NAME    = "o";

	public static final int LIMIT_LARGE_NUMBER = 100000000;
	public static final String ADD = "+";
	public static final String SUBTRACT = "-";
	public static final String LIKE = " LIKE ";
	public static final String LANG_MATCHES = " LIKE ";

	// names which cannot be used for columns and their replacements
	public static HashMap<String, String> restrictedNames = new HashMap<String, String>();
	static {
		restrictedNames.put("comment", "comme");
		restrictedNames.put("date", "dat");
		restrictedNames.put("?comment", "comme");
		restrictedNames.put("?date", "dat");
	}

	// Suppress default constructor for noninstantiability
	private Tags() {
	}

}
