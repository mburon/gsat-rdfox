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

import static com.google.common.base.Preconditions.checkState;

/**
 * StopWatch used with timing the execution of functions.
 */
public class StopWatch {

	/** The last tick. */
	private long tick;
	
	/** The total time. */
	private long total;
	
	/** Whether the stop watch is currently paused. */
	private boolean paused = false;
	
	/**
	 * Starts the stop watch.
	 */
	public void start() {
		this.resume();
		this.total = 0;
		this.paused = false;
	}

	/**
	 * Resumes the stop watch.
	 */
	public void resume() {
		this.tick = System.currentTimeMillis();
		this.paused = false;
	}
	
	/**
	 * Pauses the stop watch.
	 */
	public void pause() {
		this.paused = true;
	}

	/**
	 * @return the amount of times spent since the last lap or start whichever is the latest.
	 */
	public long lap() {
		checkState(!paused, "Attempting to apply tick to paused stop watch");
		long now = System.currentTimeMillis();
		long lastLap = tick;
		tick = now;
		long result = now - lastLap;
		total += result;
		return result;
	}

	/**
	 * @return the total time spent since the start of the stop match.
	 */
	public long total() {
		return total;
	}
}
