package com.detica.cyberreveal.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Max's Changes
 *
 * 1. Declare two new static variables creating a CONSTANT value for the tuples key and value
 *
 * 2. Use Javas shorthand operator for these kinds of assignment statements.
 *
 * 3. Removed "this" as it's implied it's referrering to the class members.
 *
 * 4.
 */

/**
 * A Storm Bolt which takes a word as an input and outputs the word with a count
 * of the number of times the word has been seen previously.
 */
public class WordCountBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 5623239456140401639L;
	// 1.
	private static final String FIELD_WORD_KEY = "word";
	private static final String FIELD_COUNT_VALUE = "count";

	// 4.
	private final Map<String, Long> wordCounts;

	public WordCountBolt() {
		wordCounts = new HashMap<>();
	}

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector collector) {
		String word = tuple.getStringByField(FIELD_WORD_KEY);
		// 3.
		Long wordCount = wordCounts.get(word);
		// If word has not been seen before, add it to the map
		if (wordCount == null) {
			wordCount = 0L;
		}
		// 2.
		wordCount += 1;
		wordCounts.put(word, wordCount);
		collector.emit(new Values(word, wordCount));
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FIELD_WORD_KEY, FIELD_COUNT_VALUE));
	}
}
