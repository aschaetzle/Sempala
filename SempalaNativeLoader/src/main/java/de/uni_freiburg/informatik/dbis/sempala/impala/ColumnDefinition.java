package de.uni_freiburg.informatik.dbis.sempala.impala;

public class ColumnDefinition {
	
	public final String columnName;
	
	public final DataType dataType;
	
	public ColumnDefinition (final String columnName, final DataType dataType){
		this.columnName = columnName;
		this.dataType = dataType;
	}
}
