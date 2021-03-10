package com.detica.cyberreveal.storm;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.detica.cyberreveal.storm.topology.BookTopology;

/**
 * Max's Changes
 *
 * 1. Replaced the Java doc comment as "The main method" isn't very descriptive on what the method does and removed the
 *    throw exceptions as the method no longer throws anything
 *
 * 2. Implemented a quick way to make the file name unique as every time I tried to run the code I would have to delete the file
 *
 * 3. Made a new output file for my feature, which is explained in comment 12 in the BookTopology class.
 *
 * Final Note: Not all methods and classes have JavaDocs, this is something I would ensure gets picked up in a code review
 * Just didn't think it was necessary for this mini project.
 */

/**
 * Main class entry point. This should be used only for testing purposes.
 */
public final class Main {

	/**
	 * Private Constructor. this is a utility class and should not be
	 * instantiated.
	 */
	private Main() {
		// Do Nothing
	}

	// 1.
	/**
	 * Gets a topology name from the arguments and builds a tapology
	 * 
	 * @param args the arguments

	 */
	public static void main(final String[] args) {
		String topologyName = null;

		// 2.
		Long fileNamePostFix = System.currentTimeMillis() / 1000L;
		// 3.
		File specificWordCountOutputFile= new File("target/specificWordCounts-" + fileNamePostFix + ".out");
		File wordCountOutputFile = new File("target/wordCounts-" + fileNamePostFix + ".out");
		List<String> specifiedWords = Arrays.asList("gentleman", "use");

		if (args != null && args.length > 0) {
			topologyName = args[0];
		}

		BookTopology topology = new BookTopology(topologyName,
				"TestScript.txt", wordCountOutputFile, specificWordCountOutputFile, specifiedWords,  1, 20);
		topology.run();
	}
}
