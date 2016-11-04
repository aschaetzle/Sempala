package de.uni_freiburg.informatik.dbis.sempala.translator;

/** An enumeration of the data formats supported */
public enum Format {
	PROPERTYTABLE,
	SINGLETABLE;
	@Override
	public String toString() {
		return super.toString().toLowerCase();
	}
}