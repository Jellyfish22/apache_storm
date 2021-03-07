package com.detica.cyberreveal.storm.topology;

import java.io.File;
import java.io.IOException;

import backtype.storm.tuple.Fields;
import com.detica.cyberreveal.storm.bolt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.detica.cyberreveal.storm.spout.BookLineSpout;

/**
 * Max's Changes
 *
 * 1. Added missing params to Java doc and changed formatting of the text (personal preference)
 *
 * 2. Moved method parameters to one line for better readability and made the parameter variables final
 *
 * 3. Declare some new static variables creating a CONSTANT value for the spout and bolt IDs
 * 	  and re-arranged order of variables to go Log -> Consts -> other instance variables (personal preference)
 *
 * 5. Added parallelismHint to the constructor and pass that to the builder.setSpout methods, removing that magic
 * 	  number. Also adding numOfWorkers to pass into the Config remove that magic number
 *
 * 6. Removed the unnecessary try/catch block as the constructor within FilePrinterBolt no longer throws IOException due
 *    to a code refactor.
 *
 * 7. Missing printing out the exception within the log
 *
 * 8. Combined the catch statement as it's doing the same thing within the catch for better readability
 *
 * 9. Replaced the javadoc comment "The Class TestTopology." This isn't that class and doesn't provide any information.
 */

// 9.
/**
 * Compile the spouts and bolts that make up the computation into a runnable topology
 */
public final class BookTopology implements Runnable {

	// 4.
	private static final Logger LOG = LoggerFactory.getLogger(BookTopology.class);

	// 3.
	private static final String BOOK_LINE_SPOUT_ID = "line";
	private static final String SPLIT_BOLT_ID = "wordSplitter";
	private static final String COUNT_BOLT_ID = "wordCount";
	private static final String SPECIFIC_COUNT_BOLT_ID = "specificWordCount";
	private static final String FILE_REPORT_COUNT_BOLT_ID = "printWordCountToFile";
	private static final String REPORT_COUNT_BOLT_ID = "printWordCount";

	private final String topologyName;
	private final String inputFile;
	private final File wordCountOutputFile;
	private final int parallelismHint;
	private final int numOfWorkers;

	// 1.
	/**
	 * Instantiates a new book topology.
	 * 
	 * @param 	topologyName 		the topology name
	 * @param 	inputFile			the input file to be read
	 * @param	wordCountOutputFile the output file that the data will be written to
	 * @param   parallelismHint 	Initial number of executor (threads)
	 */
	// 2.
	public BookTopology(final String topologyName, final String inputFile, final File wordCountOutputFile,
						final int parallelismHint, final int numOfWorkers) {
		this.topologyName = topologyName;
		this.inputFile = inputFile;
		this.wordCountOutputFile = wordCountOutputFile;
		// 5.
		this.parallelismHint = parallelismHint;
		this.numOfWorkers = numOfWorkers;
	}

	@Override
	public void run() {
		TopologyBuilder builder = new TopologyBuilder();

		// todo:// sort that 1 parrelism out
		// Can't be paralleled
		builder.setSpout(BOOK_LINE_SPOUT_ID, new BookLineSpout());

		builder.setBolt(SPLIT_BOLT_ID, new WordSplitBolt(), parallelismHint)
				.shuffleGrouping(BOOK_LINE_SPOUT_ID);

		// TODO: talk about diff between field and shuffle groupings http://storm.apache.org/releases/current/Understanding-the-parallelism-of-a-Storm-topology.html
		builder.setBolt(COUNT_BOLT_ID, new WordCountBolt(), parallelismHint)
				.fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));

		//builder.setBolt(REPORT_COUNT_BOLT_ID, new PrinterBolt(), parallelismHint)
		//		.shuffleGrouping(COUNT_BOLT_ID);

//		builder.setBolt(SPECIFIC_COUNT_BOLT_ID, new SpecificWordCountBolt("young"), parallelismHint)
//				.shuffleGrouping(COUNT_BOLT_ID);
		// 6.
		builder.setBolt(FILE_REPORT_COUNT_BOLT_ID, new FilePrinterBolt(this.wordCountOutputFile), parallelismHint)
				.shuffleGrouping(COUNT_BOLT_ID);

		Config conf = new Config();
		conf.setDebug(true);
		conf.put("inputFile", this.inputFile);

		if (this.topologyName != null) {
			conf.setNumWorkers(numOfWorkers);

			try {
				StormSubmitter.submitTopology(this.topologyName, conf, builder.createTopology());
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				// 7.
				LOG.error("Error submitting topology", e);
				// 8.
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}
