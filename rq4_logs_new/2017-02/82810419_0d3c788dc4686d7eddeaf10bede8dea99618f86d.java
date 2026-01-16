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

package org.bop.fractals.progress;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * @author Marco Ruiz
 * @since Feb 22, 2017
 */
public class BaseProgressUpdater implements IProgressUpdater {

	private static ExecutorService progressUpdaterService = Executors.newFixedThreadPool(1);

	protected Consumer<Float> progressWriter;

	public BaseProgressUpdater(Consumer<Float> progressWriter) {
		this.progressWriter = progressWriter;
	}

	public void updateProgress(float percentageProgress) {
		progressUpdaterService.submit(() -> progressWriter.accept(percentageProgress));
	}
}