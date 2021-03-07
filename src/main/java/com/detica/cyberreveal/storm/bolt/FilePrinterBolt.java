package com.detica.cyberreveal.storm.bolt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.detica.cyberreveal.storm.bolt.exception.BoltIORuntimeException;

/**
 * Max's Changes
 *
 * 1. Removed throwing IOException as the method itself never throws an exception.
 *
 * 2. Use the "try-with-resources" syntax, which will conveniently close all the opened resources after the try or if
 *    it fails, catch block. No longer requires closing the writer.
 *
 * 3. Wrote a comment to say why this method doesn't need to be used.
 *
 * 4. Made outputFile private to achieve encapsulation so that it can't be accessible outside the class. Could throw a UnsupportException
 *
 *
 * 5. Having the two separate chained .appends() is cleaner to read and utilise the writer tha has already been instantiated.
 */

/**
 * A Storm Bolt which appends all received tuples to a specified file.
 */
public class FilePrinterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 327323938874334973L;
	// 4.
	private File outputFile;
	boolean hasWritten = false;
	/**
	 * Instantiates a new file printer bolt.
	 * 
	 * @param outputFile
	 *            the output file
	 */
	// 1.
	public FilePrinterBolt(final File outputFile) {
		this.outputFile = outputFile;
	}

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector collector) {

		// 2.
		try (FileWriter writer = new FileWriter(this.outputFile, true)){
			// 5.
			writer.append(tuple.toString()).append("\n");
		} catch (IOException e) {
			throw new BoltIORuntimeException("Error while writing to output file", e);
		}
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer ofd) {
		// 3.
		// This bolt does not emit any data
	}

}
