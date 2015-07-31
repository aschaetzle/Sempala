package sparql2impala.mapreduce.util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Contains methods for writing to a nicely
 * formatted log file.
 * @author neua
 *
 */
public class MyLogger {
	
	long startTime;
	
	FileWriter outFile;
	PrintWriter out;
	
	/**
	 * Constructor. 
	 * @param logFile The name of the log file.
	 */
	public MyLogger(String logFile){
		startTime = System.currentTimeMillis();
		try {
			outFile = new FileWriter(logFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		out = new PrintWriter(outFile);

	}

	/**
	 * Get the running time.
	 * @return The running time.
	 */
	public long getRuntime(){
		return System.currentTimeMillis() - startTime;
	}
	
	/**
	 * flags and arguments
	 */
	
	public void processOptions(){
		
	}
	public void logNewSeparator(){
		out.println(MyLoggerHelper.iterationSeparator);
	}
	
	public void log(String s){
		out.println(MyLoggerHelper.wrapInFrame(s));
	}

	/**
	 * Writes the runtime to log.
	 */
	public void logRuntime() {
		logNewSeparator();
		out.println(MyLoggerHelper.wrapInFrame("Total runtime: " + getRuntime()/1000 + "s"));
		logNewSeparator();
		
	}

	/**
	 * Flushes the log. Important in case programme crashes.
	 */
	public void flushLog() {
		out.flush();
		
	}
	/**
	 * Closes stream and log.
	 */
	public void closeLog() {
		out.close();
		
	}

}
