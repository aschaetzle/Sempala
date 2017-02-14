package de.uni_freiburg.informatik.dbis.sempala.translator.sql;

//TODO add comments
public class ExecutionPlatform {
	
	private static ExecutionPlatform exPlatform;
	private static Platform platform;
	public enum Platform{
		IMPALA,
		SPARK
	}
	
	private ExecutionPlatform(Platform platform){
	}
	
	public static ExecutionPlatform getInstance(Platform pl){
		if (exPlatform == null) {
			platform = pl;
			exPlatform = new ExecutionPlatform(platform);
			return exPlatform;
		}
		return exPlatform;
		
	}
	
	public static Platform getPlatform(){
		return platform;
	}
}
