package com.detica.cyberreveal.storm.spout;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.detica.cyberreveal.storm.bolt.exception.BoltIORuntimeException;


/**
 * Max's Changes
 *
 * 1. Removed the nested try/catch blocks and utilised the "try-with-resources" syntax, which will conveniently
 * 	  close all the opened resources after the try or if it fails, catch block. The other approach is to place the
 * 	  BufferedRead, FileReader & file into a try/catch and close the connections in the final block, but because
 * 	  both the BufferedReader and FileReader throws an IOException, another nested try/catch would be needed in the
 * 	  final part making the code look "ugly".
 *
 * 2. Removed declaring it's type in the constructor as the compiler will infer it's type from the declaration
 *
 * 3. Also changed the way it loads the file, because when we build a JAR file with Maven the Sherlock holmes file will
 *    be placed into the root directory , but in the code it was looking at src/main/resources which will not exist in production
 *    throwing a FileNotFoundException. To fix this, I just simply replaced it with loading resources from the classpath instead
 *    of a specific location.
 *
 * 4. Using the custom BoltIORunTimeException
 */

/**
 * A storm spout which reads a file and outputs each line to a separate tuple.
 */
public class BookLineSpout extends BaseRichSpout {

	private static final long serialVersionUID = -7281111950770566776L;
	private SpoutOutputCollector collector;
	private List<String> lines;

	@Override
	public void open(Map conf, final TopologyContext context, final SpoutOutputCollector spoutCollector) {
		// 2.
		this.lines = new ArrayList<>();
		this.collector = spoutCollector;

		//3.
		InputStream inputFile = getClass().getClassLoader().getResourceAsStream((String) conf.get("inputFile"));

		// 1.
		try (BufferedReader buff = new BufferedReader(new InputStreamReader(inputFile))){
			String line = buff.readLine();

			while (line != null) {
				this.lines.add(line);
				line = buff.readLine();
			}

			System.out.println(lines.size());

		} catch(IOException e){
			throw new BoltIORuntimeException("Error while reading input file", e);
		}
	}

	@Override
	public void nextTuple() {
		if (!this.lines.isEmpty()) {
			String line = this.lines.remove(0);
			this.collector.emit(new Values(line));
		}
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
}
