package de.uni_freiburg.informatik.dbis.sempala.translator;

/** An enumeration of the data formats supported */
public enum Format {
	PROPERTYTABLE,
	COMPLEX_PROPERTY_TABLE,
	COMPLEX_PROPERTY_TABLE_SPARK,
	SINGLETABLE,
	EXTVP;
	@Override
	public String toString() {
		return super.toString().toLowerCase();
	}
}