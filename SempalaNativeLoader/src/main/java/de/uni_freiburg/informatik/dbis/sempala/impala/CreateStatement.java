package de.uni_freiburg.informatik.dbis.sempala.impala;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

public final class CreateStatement {
	
	private Connection connection; 
	
	private String tablename = null;
	private Boolean external = false;
	private Boolean ifNotExists = false;
	private ArrayList<ColumnDefinition> columnDefinitions = new ArrayList<ColumnDefinition>();
	private ArrayList<ColumnDefinition> partitionDefinitions = new ArrayList<ColumnDefinition>();
	private String fieldTermintor = null;
	private String lineTermintor = null;
	private FileFormat fileFormat = null;
	private String location = null;
	private String selectStatement = null;

	public CreateStatement(Connection connection) {
		this.connection = connection;
	}

	public CreateStatement(Connection connection, String tablename) {
		this.connection = connection;
		this.tablename(tablename);
	}

	public int execute() throws IllegalArgumentException, SQLException {
		System.out.print(String.format("Creating table '%s'", tablename));
		long startTime = System.currentTimeMillis();
		int ret = connection.createStatement().executeUpdate(toString());
		long endTime = System.currentTimeMillis();
		System.out.println(String.format(" [%.3fs]", (float)(endTime - startTime)/1000));
		return ret;
	}

	public CreateStatement tablename(String tablename) {
		this.tablename = tablename;
		return this;
	}

	public CreateStatement external() {
		this.external = true;
		return this;
	}

	public CreateStatement ifNotExists() {
		this.ifNotExists = true;
		return this;
	}

	public CreateStatement addColumnDefinition(final String columnName, final DataType dataType) {
		this.columnDefinitions.add(new ColumnDefinition(columnName, dataType));
		return this;
	}

	public CreateStatement addPartitionDefinition(final String columnName, DataType dataType) {
		this.partitionDefinitions.add(new ColumnDefinition(columnName, dataType));
		return this;
	}

	public CreateStatement fieldTermintor(final String fieldTermintor) {
		this.fieldTermintor = fieldTermintor;
		return this;
	}

	public CreateStatement lineTermintor(final String lineTermintor) {
		this.lineTermintor = lineTermintor;
		return this;
	}

	public CreateStatement storedAs(final FileFormat fileFormat) {
		this.fileFormat = fileFormat;
		return this;
	}

	public CreateStatement location(final String location) {
		this.location = location;
		return this;
	}

	public CreateStatement asSelect(final String selectStatement) {
		this.selectStatement = selectStatement;
		return this;
	}

	public CreateStatement asSelect(final SelectStatement selectStatement) {
		this.selectStatement = selectStatement.toString();
		return this;
	}

	public String toString() throws IllegalArgumentException {
		if (this.tablename == null || (this.selectStatement == null && this.columnDefinitions.isEmpty()))
			throw new IllegalArgumentException("tablename and either some columnDefinitions or asSelect must be specified");
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("CREATE%s TABLE%s %s", this.external ? " EXTERNAL" : "", this.ifNotExists ? " IF NOT EXISTS" : "", this.tablename));
		if (!this.columnDefinitions.isEmpty()){
			sb.append(" (");
			Iterator<ColumnDefinition> it = this.columnDefinitions.iterator();
			ColumnDefinition columnDefinition = it.next();
			sb.append(String.format("\n\t%s %s", columnDefinition.columnName, columnDefinition.dataType.name()));
		    while (it.hasNext()) {
				columnDefinition = it.next();
				sb.append(String.format(",\n\t%s %s", columnDefinition.columnName, columnDefinition.dataType.name()));
		    }
			sb.append("\n)");
		}
		if (!this.partitionDefinitions.isEmpty()){
			sb.append("\nPARTITIONED BY (");
			Iterator<ColumnDefinition> it = this.partitionDefinitions.iterator();
			ColumnDefinition columnDefinition = it.next();
			sb.append(String.format("%s %s", columnDefinition.columnName, columnDefinition.dataType.name()));
		    while (it.hasNext()) {
				columnDefinition = it.next();
				sb.append(String.format(", %s %s", columnDefinition.columnName, columnDefinition.dataType.name()));
		    }
			sb.append(")");
		}
		if (this.fieldTermintor != null || this.lineTermintor != null) {
			sb.append("\nROW FORMAT DELIMITED");
			if (this.fieldTermintor != null)
				sb.append(String.format(" FIELDS TERMINATED BY '%s'", this.fieldTermintor));
			if (this.lineTermintor != null)
				sb.append(String.format(" LINES TERMINATED BY '%s'", this.lineTermintor));
		}
		if (this.fileFormat != null)
			sb.append(String.format(" STORED AS %s", this.fileFormat.name()));
		if (this.location != null)
			sb.append(String.format("\nLOCATION '%s'", this.location));
		if (this.selectStatement != null)
			sb.append(String.format("\nAS\n%s", this.selectStatement));
		return sb.toString();
	}

}
