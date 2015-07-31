package com.antony_neu.thesis.RDFToCSV;

import java.io.IOException;
import java.util.ArrayList;

import javax.swing.table.AbstractTableModel;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WideTableReducer extends
		Reducer<CompositeKey, CompositeKey, Text, Text> {

	Impala wrapper = new Impala();
	MultipleOutputs<Text, Text> mos;
	final Text emptyText = new Text();

	protected void setup(Context context) throws IOException {
		mos = new MultipleOutputs<Text, Text>(context);
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

	public void reduce(CompositeKey key, Iterable<CompositeKey> values,
			Context context) throws IOException, InterruptedException {

		String prev = "";
		String tablename = key.getFirst().toString();
		ArrayList<ArrayList<String>> table = new ArrayList<ArrayList<String>>();
		ArrayList<String> colNames = new ArrayList<String>();

		int index = -1;
		for (CompositeKey value : values) {
			if (value.getFirst().toString().equals(prev)) {
				table.get(index).add(value.getSecond().toString());
			} else {
				index++;
				colNames.add(value.getFirst().toString());
				table.add(new ArrayList<String>());
				table.get(index).add(value.getSecond().toString());
				prev = value.getFirst().toString();
			}
			prev = value.toString();
		}
		
//		// output table row by row
//		boolean emptyRow = false;
//		int rowIndex = -1;
//		StringBuilder rowString;
//		while(!emptyRow){
//			emptyRow = true;
//			rowString = new StringBuilder("");
//			rowIndex++;
//			for(ArrayList<String> column : table){
//				try{
//					rowString.append(column.get(rowIndex));
//					rowString.append("\t");
//					emptyRow = false;
//				} catch(IndexOutOfBoundsException exception){
//					// do nothing
//					mos.write(new Text("index: "+rowIndex), emptyText, "errorLog");
//					rowString.append("\t");
//					
//				}
//			}
//			rowString.append("\n");
//			if (!emptyRow) {
//				mos.write(new Text(rowString.toString()), emptyText, tablename.replace(':', '_'));
//			}
//			mos.write(new Text(colNames.toString()), emptyText, ("colnames-"+tablename).replace(':', '-'));
//		}


	}

}
