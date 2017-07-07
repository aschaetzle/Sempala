package de.uni_freiburg.informatik.dbis.sempala.translator.sql;

/**
 * The translator works both for Impala or for Spark if the format is set to
 * complex property table. This class represents a platform for which the
 * queries will be translated.
 *
 * @author Polina Koleva
 *
 */
public class ExecutionPlatform {

	private static ExecutionPlatform exPlatform;

	private static Platform platform;

	/**
	 * If the format is complex_property_table, the queries can be translated
	 * either for Spark or for Impala. So both platforms are supported by the
	 * translator.
	 * 
	 * @author Polina Koleva
	 *
	 */
	public enum Platform {
		IMPALA, SPARK
	}

	// private constructor because of singleton pattern
	private ExecutionPlatform(Platform platform) {
	}

	public static ExecutionPlatform getInstance(Platform pl) {
		if (exPlatform == null) {
			platform = pl;
			exPlatform = new ExecutionPlatform(platform);
			return exPlatform;
		}
		return exPlatform;

	}

	/**
	 * Get the current platform for which the queries are translated.
	 */
	public static Platform getPlatform() {
		return platform;
	}
}
