package com.detica.cyberreveal.storm.bolt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.detica.cyberreveal.storm.bolt.exception.BoltIORuntimeException;

/**
 * Max's Changes
 *
 * 1. Removed throwing IOException from the constructor as the method itself never throws an exception.
 *
 * 2. Use the "try-with-resources" syntax, which will conveniently close all the opened resources after the try or if
 *    it fails in the catch block. So it no longer requires manually closing the writer.
 *
 * 3. Wrote a comment to say why this method doesn't have any implementation.
 *
 * 4. Made outputFile private to achieve encapsulation so that it can't be accessible outside the class.
 *
 * 5. Having the two separate chained appends() is cleaner to read and utilises the writer that has already been instantiated.
 *
 * 6. Fixed spelling in comment.
 *
 * 7. Extending BasicBaseBolt exposes us to a method called cleanup() from the IBolt interface. Storm will call the cleanup
 *    method when the bolt is going to shut down, therefore the content of the file will be the final count of words, instead
 *    of each threads count. To note: this isn't the how the cleanup method should typically be used, it's more for releasing
 *    resources use by the bolt, an example could be closing down database connections etc.
 */

// 6.
/**
 * A Storm Bolt which appends all received tuples to a specified file.
 */
public class FilePrinterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 327323938874334973L;
	// 4.
	private File outputFile;

	private HashMap<String, Long> tupleCount;

	/**
	 * Instantiates a new file printer bolt.
	 * 
	 * @param outputFile
	 *            the output file
	 */
	// 1.
	public FilePrinterBolt(final File outputFile) {
		this.outputFile = outputFile;
		tupleCount = new HashMap<>();
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
		tupleCount.put(tuple.getStringByField(WordCountBolt.FIELD_WORD_KEY), tuple.getLongByField(WordCountBolt.FIELD_COUNT_VALUE));
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer ofd) {
		// 3.
		// This bolt does not emit any data
	}

	// 7.
	@Override
	public void cleanup(){
		// 2.
		try (FileWriter writer = new FileWriter(this.outputFile, true)){
			for(Map.Entry<String, Long> entry : tupleCount.entrySet()){
				// 5.
				writer.append(entry.getKey() + " : " + entry.getValue()).append("\n");
			}
		} catch (IOException e) {
			throw new BoltIORuntimeException("Error while writing to output file", e);
		}
	}
}
