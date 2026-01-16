/*
 * Copyright 2002-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bop.fractals;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author Marco Ruiz
 * @since Feb 21, 2017
 */
public abstract class GeometricFractalBuilder<GEOMETRY_T> implements IFractalBuilder {

	private ProgressUpdateWorker progressUpdateWorker;
	protected List<GEOMETRY_T> output = new ArrayList<>();

	protected boolean interrupted = false;
	protected boolean computing = false;
	private long totalNumGeometries;

	public GeometricFractalBuilder(Consumer<Double> progressUpdater) {
		progressUpdateWorker = new ProgressUpdateWorker(this, progressUpdater);
	}

	public void run() {
		computing = true;
		totalNumGeometries = computeNumGeometriesToCompute();
		output = new ArrayList<>();
		new Thread(progressUpdateWorker).start();
		buildFractal();
		progressUpdateWorker.stop();
		computing = false;

		progressUpdateWorker.updateProgress(100);
	}

	public abstract void buildFractal();
	public abstract long computeNumGeometriesToCompute();

	public boolean isComputing() {
		return computing;
	}

	public void reset() {
		output.clear();
	}

	public void stop() {
		interrupted = true;
	}

	public List<GEOMETRY_T> getOutput() {
		return output;
	}

	public double getPercentageProgress() {
		return 100 * output.size() / totalNumGeometries;
	}

	public long getTotalNumGeometries() {
		return totalNumGeometries;
	}
}
