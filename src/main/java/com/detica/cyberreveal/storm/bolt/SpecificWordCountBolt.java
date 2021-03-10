package com.detica.cyberreveal.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A storm bolt to take a list of words to return the count of occurrences
 */
public class SpecificWordCountBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -375275649411485184L;

    private final List<String> specificWords;
    private final Map<String, Long> wordCounts;

    public SpecificWordCountBolt(List<String> specificWords){
        this.specificWords = specificWords;
        wordCounts = new HashMap<>();
    }

    @Override
    public void execute(final Tuple tuple, final BasicOutputCollector collector) {
        String name = tuple.getStringByField(WordCountBolt.FIELD_WORD_KEY);
        Long count = tuple.getLongByField(WordCountBolt.FIELD_COUNT_VALUE);

        for(String specificWord : specificWords){
            if(specificWord.equals(name)){
                wordCounts.put(name, count);
                collector.emit(new Values(name, count));
            }
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(WordCountBolt.FIELD_WORD_KEY, WordCountBolt.FIELD_COUNT_VALUE));
    }
}
