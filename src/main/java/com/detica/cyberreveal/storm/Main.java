package com.detica.cyberreveal.storm;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

import com.detica.cyberreveal.storm.topology.BookTopology;

/**
 * Max's Changes
 *
 * 1. Removed private constructor
 *
 * 2. Replaced the Java doc comment as "The main method" isn't very descrpitive on what the method does and removed the
 * throw exceptions as the method no longer throws anything
 */

/**
 * Main class entry point. This should be used only for testing purposes.
 */
public final class Main {

	// 1.

	// 2.
	/**
	 * Gets a topology name from the arguments and builds a tapology
	 * 
	 * @param args the arguments

	 */
	public static void main(final String[] args) {
		String topologyName = null;

		if (args != null && args.length > 0) {
			topologyName = args[0];
		}

		// TODO: // 3. when mvn packages this jar, it doesnt unpack into src/resource/
		BookTopology topology = new BookTopology(topologyName,
				"AdventuresOfSherlockHolmes.txt", new File(
						"target/wordCounts.out" + System.currentTimeMillis() / 1000L), 1, 20);
		topology.run();
	}
}
