package sparql2impala.mapreduce.RDFToCSV;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import sparql2impala.mapreduce.util.ConfigConstants;
import sparql2impala.mapreduce.util.MyLogger;

/**
 * Converts input rdf file to csv files. Format of these files can be chosen by
 * command line arguments.
 * 
 * Driver class.
 * 
 * @author neua
 * 
 */
public class RDFConverter {

	// main method
	public static void main(String[] args) throws Exception {

		// the hadoop configuration for all jobs
		Configuration conf = new Configuration();
		ArgumentsProcessor processor = new ArgumentsProcessor(args);

		// remaining arguments
		String[] otherArgs = processor.getOtherArgs();
		if (otherArgs.length != 3) {
			System.err
					.println("Usage: RDFConverter [OPTION] [INPUT] [NUMREDUCER] [DATASETNAME]");
			processor.outputUsage();
			System.exit(2);
		}

		int numReducer = Integer.parseInt(otherArgs[1]);
		String datasetName = otherArgs[2];

		boolean logging = processor.isLogging();
		boolean enableLubmPreprocessing = processor.isLubm();

		// read table format, i.e. vertical partitioning
		TableFormat format = processor.getFormat();

		conf.set(ConfigConstants.ISNQUADSKEY, "DISABLED");

		// logging active?
		if (logging) {
			System.out.println("Logging enabled.");
			conf.set(ConfigConstants.loggingKey, ConfigConstants.loggingEnabled);
		} else {
			System.out.println("Logging disabled.");
			conf.set(ConfigConstants.loggingKey, ConfigConstants.loggingEnabled);
		}

		String logFile = processor.getLogFile();

		MyLogger logger = new MyLogger(logFile);

		if (logging) {
			System.out.println("Logging to file " + logFile);
			logger.logNewSeparator();
			logger.log("Input file: " + otherArgs[0]);
		}

		/**
		 * Start of actual progamme.
		 */

		Job job;

		if (processor.isNquads()) {
			System.out.println("File format is n quads.");
			conf.set(ConfigConstants.ISNQUADSKEY,
					ConfigConstants.ISNQUADSENABLES);
		} else {
			conf.set(ConfigConstants.ISNQUADSKEY, "DISABLED");
		}

		// Create job depending on table format
		// vertical partitioning
		if (format == TableFormat.VERTICAL) {
			job = new Job(conf,
					"Conversion from RDF to TSV using vertical partitioning "
							+ otherArgs[0]);
			job.setJarByClass(RDFConverter.class);
			job.setMapperClass(VerticalPartitioningMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(TextCompositeKey.class);
			job.setReducerClass(VerticalPartitioningReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(numReducer);
		} else if (format == TableFormat.TRIPLESTORE) {
			if (enableLubmPreprocessing) {
				conf.set("REPLACE_PREFIXES", "TRUE");
			}
			// triple store
			job = new Job(conf, "Conversion from RDF to TSV as triple store "
					+ otherArgs[0]);
			job.setJarByClass(RDFConverter.class);
			job.setMapperClass(TripleStoreMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(TextCompositeKey.class);
			job.setNumReduceTasks(0);
		} else {
			// preprocessing
			conf.set("datasetName", datasetName);

			if (format == TableFormat.PARTITIONED_BIGTABLE) {
				conf.set("IS_PARTITIONED_BY_TYPE", "TRUE");
			}

			// if LUBM dataset , then run pre-preprocessing
			if (enableLubmPreprocessing) {
				job = new Job(conf, "LUBM Preprocessing " + otherArgs[0]);
				job.setJarByClass(RDFConverter.class);
				job.setMapperClass(LUBMPreprocessingMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setReducerClass(LUBMPreprocessingReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);

				FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
				FileOutputFormat.setOutputPath(job, new Path(datasetName
						+ "_lubm_preprocessing"));
				job.setNumReduceTasks(numReducer);
				job.waitForCompletion(true);
			}

			// preprocessing
			job = new Job(conf, "Preprocessing " + otherArgs[0]);
			job.setJarByClass(RDFConverter.class);
			job.setMapperClass(PreprocessingMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(PreprocessingReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			if (enableLubmPreprocessing) {
				FileInputFormat.addInputPath(job, new Path(datasetName+ "_lubm_preprocessing"));
			} else {
				FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			}
			FileOutputFormat.setOutputPath(job, new Path(datasetName
					+ "_preprocessing"));
			
			job.setNumReduceTasks(numReducer);
			job.waitForCompletion(true);

			// actual job
			conf.set("datasetName", datasetName);
			job = new Job(conf,
					"Conversion from RDF to TSV using property table "
							+ otherArgs[0]);

			job.setJarByClass(RDFConverter.class);
			job.setMapperClass(WideTableMapper.class);
			job.setMapOutputKeyClass(CompositeKey.class);
			job.setMapOutputValueClass(CompositeKey.class);
			job.setReducerClass(WideTableReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			// secondary sort
			job.setPartitionerClass(CompositeKeyPartitioner.class);
			job.setSortComparatorClass(CompositeKeyComparator.class);
			job.setGroupingComparatorClass(CompositeGroupComparator.class);

			job.setNumReduceTasks(numReducer);
		}
		
		if (enableLubmPreprocessing && format == TableFormat.WIDETABLE) {
			FileInputFormat.addInputPath(job, new Path(datasetName+ "_lubm_preprocessing"));
		} else {
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		}
		FileOutputFormat.setOutputPath(job, new Path(datasetName));
		job.waitForCompletion(true);

		/**
		 * Logging
		 */
		if (logging) {
			logger.logNewSeparator();

			long triplesCount = job.getCounters()
					.findCounter(MyCounters.triplesCount).getValue();
			logger.log("Number of edges: " + triplesCount);
			logger.flushLog();
		}

		logger.logRuntime();

		// close buffer
		logger.closeLog();
	}

}
