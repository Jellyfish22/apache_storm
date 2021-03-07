package com.detica.cyberreveal.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Max's Changes
 *
 * 1. I would normally replace system.out with a Logger as the tuple being passed in can contain sensitive data, which must be logged
 * 	  securely. System.out is also slow which can affect your speed of code, especially if you're benchmarking something.
 * 	  I chose not to replace it as the class comment explicity states the functionality is to print it to System.out
 *
 * 2. Throw an UnsupportedOperationException as the FilePrinterBolt does not emit any data.
 */

/**
 * A Storm Bolt which prints all received tuples to System.out
 */
public class PrinterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -5237229359039158290L;

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector collector) {
		// 1.
		System.out.println(tuple);
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer ofd) {
		// 2.
		// throw new UnsupportedOperationException("This bolt does not emit any data");
	}
}
