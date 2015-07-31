package sparql2impala.mapreduce.util;

public class MyLoggerHelper {

	public static String iterationSeparator = "";
	public static final int LINELENGTH = 70;

	static {
		for (int i = 0; i < LINELENGTH; i++) {
			iterationSeparator += "+";
		}
	}
	
	public static String getSpaces(int spaces){
		String result = "";
		for(int i = 0; i<spaces; i++){
			result += " ";
		}
		return result;
	}
	
	public static String wrapInFrame (String s){
		return "+ "+s+getSpaces(LINELENGTH-s.length()-4)+" +";
	}

}
