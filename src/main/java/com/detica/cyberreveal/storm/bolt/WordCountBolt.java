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
 * 1. Declare two new static variables creating a constant value for the tuples key/value, these are used by almost every
 *    Storm Bolt so, by declaring them public other bolts can use them and if the need to change the values then you only need
 *    to change it in one place.
 *
 * 2. Use Javas shorthand operator for these kinds of assignment statements.
 *
 * 3. Removed "this" as it's implied it's referring to the class members, I also think it makes the code more readable.
 *
 * 4. Added the initialisation in the constructor as oppose to the top where it's declared. Personal preference, I'm curious
 *    to know your preference on this.
 */

/**
 * A Storm Bolt which takes a word as an input and outputs the word with a count
 * of the number of times the word has been seen previously.
 */
public class WordCountBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 5623239456140401639L;
	// 1.
	public static final String FIELD_WORD_KEY = "word";
	public static final String FIELD_COUNT_VALUE = "count";

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
