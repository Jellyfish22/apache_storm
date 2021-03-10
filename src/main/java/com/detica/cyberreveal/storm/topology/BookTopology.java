package com.detica.cyberreveal.storm.topology;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
 * 1. Added missing params to Java doc and changed formatting of the text for readability
 *
 * 2. Moved method parameters to one line for better readability and made the parameter variables final as once they are
 * 	  initialised they do not need to be reassigned.
 *
 * 3. Declare some new static variables creating a CONSTANT value for the spout and bolt IDs
 * 	  and re-arranged order of variables to go Log -> Consts -> other instance variables for easier readability
 *
 * 5. Added parallelismHint to the constructor and pass that to the builder.setSpout methods, removing that magic
 * 	  number. Also adding numOfWorkers to pass into the Config remove that magic number
 *
 * 6. Removed the unnecessary try/catch block as the constructor within FilePrinterBolt no longer throws IOException due
 *    to a code refactor.
 *
 * 7. No longer swallowing the exception within the log
 *
 * 8. Combined the catch statement as it's doing the same thing within the catch for better readability
 *
 * 9. Replaced the javadoc comment "The Class TestTopology." as this isn't that class and doesn't provide any information.
 *
 * 10. This was set to 10000 which submits the topology runs for 10 seconds and then kills it. Resulting in the program
 *     not finishing going through the Sherlock Holmes text, so set it to 60 seconds to give it enough time. Ideally, there
 *     would be a callback to notify when it's done so you don't have to hard code a specific time. But in reality, we're not
 *     using Storm as it's intended to be, in short this is similar to batch processing and not utilising Storms realtime benefits.
 *
 * 11. In this case, BookSpout can't be paralleled due to the input data being a text file. Attempting to run this in parallel
 *     will result in duplicate data as the framework will be sending off the same text document to different threads. This could be
 *     improved by potentially reading off of a queue of some kind, with this approach when a spout reads from it, it can be
 *     cleared so that no two workers can get the same data.
 *
 * 12. This is my feature, it essentially takes in a list of words you specify and returns the count of these words and writes it to a
 *     file. The process goes as follows:
 *     		Receives data from Book Line spout
 *     		Splits the words by the WordSplitBolt and passes the data to the CountBolt
 *     		CountBolt counts the number of times all words occur
 *     		SpecificWordCountBolt searches this data passed from CountBlt to return the count of those words
 *     		FilePrinterBolt will write these tuples to a file.
 *
 * 13. Fixed bug by removing shuffleGrouping() and replacing it with fieldsGrouping(). shuffle grouping was randomly
 *     distributing tuples across the targets bolts. FieldsGrouping will route the tuples
 *     with the same field name to the same worker, so all the Strings of "Gentleman" will end up on the same worker which
 *     is important because each worker is holding a HashMap with a count for each word so if the same word goes to 2 workers
 *     then they will not know about the other workers count and you get 2 counts that are wrong.
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
	private static final String FILE_REPORT_SPECIFIC_COUNT_BOLT_ID = "printSpecificWordCountToFile";
	private static final String REPORT_COUNT_BOLT_ID = "printWordCount";

	private final String topologyName;
	private final String inputFile;
	private final File wordCountOutputFile;
	private final File specificWordCountOutputFile;
	private final int parallelismHint;
	private final int numOfWorkers;

	private List<String> specifiedWords;
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
						final File specificWordCountOutputFile, final List<String> specifiedWords, final int parallelismHint, final int numOfWorkers) {
		this.topologyName = topologyName;
		this.inputFile = inputFile;
		this.wordCountOutputFile = wordCountOutputFile;
		this.specificWordCountOutputFile = specificWordCountOutputFile;
		this.specifiedWords = specifiedWords;
		// 5.
		this.parallelismHint = parallelismHint;
		this.numOfWorkers = numOfWorkers;
	}

	@Override
	public void run() {
		TopologyBuilder builder = new TopologyBuilder();

		// 11.
		// Can't be paralleled
		builder.setSpout(BOOK_LINE_SPOUT_ID, new BookLineSpout());

		builder.setBolt(SPLIT_BOLT_ID, new WordSplitBolt(), parallelismHint)
				.shuffleGrouping(BOOK_LINE_SPOUT_ID);

		// 13.  talk about diff between field and shuffle groupings http://storm.apache.org/releases/current/Understanding-the-parallelism-of-a-Storm-topology.html
		builder.setBolt(COUNT_BOLT_ID, new WordCountBolt(), parallelismHint)
				.fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));

		// 12.
		builder.setBolt(SPECIFIC_COUNT_BOLT_ID, new SpecificWordCountBolt(specifiedWords), parallelismHint)
				.shuffleGrouping(COUNT_BOLT_ID);

		builder.setBolt(REPORT_COUNT_BOLT_ID, new PrinterBolt(), parallelismHint)
				.shuffleGrouping(COUNT_BOLT_ID);

		// 6.
		builder.setBolt(FILE_REPORT_COUNT_BOLT_ID, new FilePrinterBolt(this.wordCountOutputFile), parallelismHint)
				.shuffleGrouping(COUNT_BOLT_ID);

		builder.setBolt(FILE_REPORT_SPECIFIC_COUNT_BOLT_ID, new FilePrinterBolt(specificWordCountOutputFile), parallelismHint)
				.shuffleGrouping(SPECIFIC_COUNT_BOLT_ID);

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
			// 10.
			Utils.sleep(60000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}
