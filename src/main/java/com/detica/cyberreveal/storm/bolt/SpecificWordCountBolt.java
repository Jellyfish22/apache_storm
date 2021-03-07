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

// TODO: CHANGE TO THE FIELD GROUPING THINGY
public class SpecificWordCountBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -375275649411485184L;

    private final Map<String, Long> wordCounts;
    private final List<String> specificWords;

    public SpecificWordCountBolt(List<String> specificWords){
        this.specificWords = specificWords;
        wordCounts = new HashMap<>();
    }

    @Override
    public void execute(final Tuple tuple, final BasicOutputCollector collector) {
        String name = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");

        for(String specificWord : specificWords){
            if(specificWord.equals(name)){
                wordCounts.put(name, count);
                collector.emit(new Values(name, count));
            }
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
