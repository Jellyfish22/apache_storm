package com.detica.cyberreveal.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Max's Changes
 *
 * 1. I would normally replace system.out with a Logger as the tuple being passed in can contain sensitive data, which must be logged
 * 	  securely. System.out is also slow which can affect the speed of the code, especially if you're benchmarking something.
 * 	  I chose not to replace it as the class comment explicitly states that the functionality is to print to System.out.
 *
 * 2. Wrote a comment to say why this method doesn't have any implementation.
 */

/**
 * A Storm Bolt which prints all received tuples to System.out
 */
public class PrinterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -5237229359039158290L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
		// 1.
		System.out.println(tuple);
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer ofd) {
		// 2.
		// This bolt does not emit any data
	}
}
