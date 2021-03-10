package com.detica.cyberreveal.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Max's Changes
 *
 * 1. Changed the previous regex to: if we see anything that isn't a word then split
 * 	  ^ = when placed inside brackets it means "not".
 * 	  \\w = split anything that's a word character
 *
/**
 * A storm bolt which splits a line into words.
 */
public class WordSplitBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1990152678196466476L;

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector collector) {
		String line = tuple.getStringByField("line");
		// split line by whitespace and punctuation characters
		// 1.
		String[] words = line.split("[^\\w]");
		for (int i = 0; i < words.length; i++) {
			String word = words[i].toLowerCase().trim();
			if (word.length() > 0) {
				collector.emit(new Values(word));
			}
		}
		System.out.println();
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}