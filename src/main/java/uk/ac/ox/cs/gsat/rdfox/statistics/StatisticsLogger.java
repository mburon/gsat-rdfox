/*
 * Copyright (C) 2018 National Institute of Advanced Industrial Science and Technology (AIST),
 * Inria, École Polytechnique, Universidade de Lisboa.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use the file in this project except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * The name of the copyright holders may not be used to endorse or promote products
 * derived from this software without specific prior written permission.
 */
package uk.ac.ox.cs.gsat.rdfox.statistics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;

/**
 * StatisticsLogger, without the heavy lifting of Log4J or java.util.logging
 *
 * @author Julien Leblay
 * @author Maxime Buron
 */
public class StatisticsLogger implements AutoCloseable, Observer {

	/** The output print stream. */
	private final PrintStream out;

	/** The outputs. */
	private final StatisticsCollector stats;

	/** The command line properties. */
	private final Properties commandLine;
	
	/** Whether the prolog has already been printed or not. */
	private boolean hasPrintedProlog = false;
	
	/** The global start time. */
	private final long globalStart = System.currentTimeMillis();

	/** The default value for printing out missing values. */
	private static final String MISSING_VALUE = "N/A";
	
	/** The last printed header. */
	private Set<StatisticsColumn> lastHeader = new HashSet<>();

    protected List<StatisticsColumn> sortedHeader = new ArrayList<>();
	
	/**
	 * Instantiates a new logger.
	 *
	 * @param stats the statistics collection object
	 */
	public StatisticsLogger(StatisticsCollector stats) {
		this(System.out, stats, new Properties());
	}
	
	/**
	 * Instantiates a new logger.
	 *
	 * @param stats the statistics collection object
	 */
	public StatisticsLogger(StatisticsCollector stats, Properties cmdLine) {
		this(System.out, stats, cmdLine);
	}

    /**
	 * Instantiates a new logger.
	 *
	 * @param out the output print stream
	 * @param stats the statistics collection object
	 */
	public StatisticsLogger(PrintStream out, StatisticsCollector stats) {
        this(out, stats, new Properties());
	}

	/**
	 * Instantiates a new logger.
	 *
	 * @param out the output print stream
	 * @param stats the statistics collection object
	 */
	public StatisticsLogger(PrintStream out, StatisticsCollector stats, Properties cmdLine) {
		this.out = out;
		this.stats = stats;
		this.commandLine = cmdLine;
		this.stats.addObserver(this);
	}

	/**
	 * Prints some system environment information to the given output.
	 * 
	 * @param out
	 */
	private static void printVersionInfo(PrintStream out) {
		StringBuilder result = new StringBuilder();
		result.append("#\n# ").append("Version").append("\n#");
		Runtime runtime = Runtime.getRuntime();
		try {
			Process process = runtime.exec("git rev-parse --abbrev-ref HEAD");
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
				result.append("\n# Branch: ").append(reader.readLine());
			}
			process = runtime.exec("git rev-parse HEAD");
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
				result.append("\n# ChangeSet: ").append(reader.readLine());
			}
		} catch (IOException e) {
			result.append("\n# Could not retrieve version information");
		}
		out.println(result.toString());
	}

	/**
	 * Prints some system environment information to the given output.
	 * 
	 * @param out
	 */
	private static void printSystemSettings(PrintStream out) {
		StringBuilder result = new StringBuilder();
		result.append("#\n# ").append("System").append("\n#");
		result.append("\n# StartTime: " + new Date(System.currentTimeMillis()));
		result.append("\n# OS: " + System.getProperty("os.name"));
		result.append("\n# OSArch: " + System.getProperty("os.arch"));
		result.append("\n# OSVersion: " + System.getProperty("os.version"));
		result.append("\n# StartUpMem: " + (Runtime.getRuntime().totalMemory() / 1000000) + "MB");
		result.append("\n# StartUpMaxMem: " + (Runtime.getRuntime().maxMemory() / 1000000) + "MB");
		result.append("\n# NumCPUs: " + Runtime.getRuntime().availableProcessors());
		out.println(result.toString());
	}

	/**
	 * Prints the experiment's command line input parameters to the given output.
	 * 
	 * @param out
	 * @param params
	 */
	private static void printCommandLineParameters(PrintStream out, Properties config) {
		StringBuilder result = new StringBuilder();
		result.append("#\n# ").append("Command line params").append("\n#");
		for (Object key: config.keySet()) {
			result.append("\n# ").append(key).append('=').append(config.get(key));
		}
		result.append("\n# ");
		out.println(result.toString());
		// log.info(result.toString());
	}

	/**
	 * Logs a message with the given verbosity level.
	 *
	 */
	public void printProlog() {
		printSystemSettings(out);
		printCommandLineParameters(out, this.commandLine);
	}

	/**
	 * Logs a message with the given verbosity level.
	 *
	 */
	public void printHeader() {
		List<String> result = new ArrayList<String>();
		// ensure INPUT is part of the header
		result.add("INPUT");
        for (StatisticsColumn col : sortHeader())
            result.add(col.name());
		out.println(Joiner.on('\t').join(result));
	}
	
	/**
	 * Sorts the header in alphanumeric order.
	 *
	 * @return the set of header string sorted
	 */
	private List<StatisticsColumn> sortHeader() {
        if (this.sortedHeader != null){
            return this.sortedHeader;
        } else {
            List<StatisticsColumn> headers = new ArrayList<>();
            headers.addAll(stats.cells().columnKeySet());
            Collections.sort(headers.stream().map(h -> h.name()).collect(Collectors.toList()));
            return headers;
        }
	}

    public void setSortedHeader(List<StatisticsColumn> sortedHeader) {
        this.sortedHeader = sortedHeader;
    }

	/**
	 * Prints the given row.
	 *
	 * @param rowName the name of the row to print
	 */
	public void printRow(String rowName) {
		Map<StatisticsColumn, Object> row = this.stats.cells().row(rowName);
		StringBuilder result = new StringBuilder(rowName);
		for (StatisticsColumn col: sortHeader()) {
			result.append('\t').append(row.getOrDefault(col, MISSING_VALUE));
		}
		out.println(result);
	}

	/**
	 * Prints the all rows.
	 */
	public void printAll() {
		// Ensuring the column order is preserve across lines.
		List<StatisticsColumn> cols = sortHeader();
		for (Entry<String, Map<StatisticsColumn, Object>> entry: this.stats.cells().rowMap().entrySet()) {
			StringBuilder result = new StringBuilder(entry.getKey());
			Map<StatisticsColumn, Object> row = entry.getValue();
			for (StatisticsColumn col: cols) {
				result.append('\t').append(row.getOrDefault(col, MISSING_VALUE));
			}
			out.println(result);
		}
	}

	/**
	 * @return the output stream of this logger
	 */
	public StatisticsCollector stats() {
		return this.stats;
	}

	/**
	 * @return the output stream of this logger
	 */
	public PrintStream out() {
		return out;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.AutoCloseable#close()
	 */
	@Override
	public void close() {
		out.println("Total time: " + (System.currentTimeMillis() - globalStart) + " ms.");
		if (this.out != System.out) {
			this.out.close();
		}
	}

	/**
	 * {@inheritDoc}
	 * @see java.util.Observer#update(java.util.Observable, java.lang.Object)
	 */
	@Override
	public void update(Observable o, Object arg) {
		if (!hasPrintedProlog) {
			printProlog();
			hasPrintedProlog = true;
		}
		Set<StatisticsColumn> header = this.stats.cells().columnKeySet();
		if (!header.equals(lastHeader)) {
			lastHeader = new HashSet<StatisticsColumn>();
            lastHeader.addAll(header);
			printHeader();
		}
		if (arg != null) {
			printRow((String) arg);
		} else {
			printAll();
		}
	}
}
