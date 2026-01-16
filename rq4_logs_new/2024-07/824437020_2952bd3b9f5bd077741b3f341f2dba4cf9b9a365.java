/*
 * Copyright (C) 2024 DiffPlug
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.diffplug.webtools.node;

import com.github.eirslett.maven.plugins.frontend.lib.ProxyConfig;
import java.util.Collections;
import java.util.Objects;
import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;

/**
 * Installs a specific version of node.js and npm,
 * Runs 'npm ci' to install the packages, and then `gulp blah`
 * to run whatever gulp tasks you want.
 */
public class NodePlugin implements Plugin<Project> {
	private static final String EXTENSION_NAME = "node";

	public static class Extension {
		private final Project project;

		public Extension(Project project) {
			this.project = Objects.requireNonNull(project);
		}

		public final SetupCleanupNode setup = new SetupCleanupNode();

		public TaskProvider<?> npm_run(String name, Action<Task> taskConfig) {
			return project.getTasks().register("npm_run_" + name, NpmRunTask.class, task -> {
				task.taskName = name;
				task.getSetup().set(setup);
				task.getProjectDir().set(project.getProjectDir());
				task.getInputs().file("package-lock.json").withPathSensitivity(PathSensitivity.RELATIVE);

				task.getInputs().property("nodeVersion", setup.nodeVersion);
				task.getInputs().property("npmVersion", setup.npmVersion);
				taskConfig.execute(task);
			});
		}
	}

	@CacheableTask
	public abstract static class NpmRunTask extends DefaultTask {
		public String taskName;

		@Input
		public String getTaskName() {
			return taskName;
		}

		@Internal
		public abstract Property<SetupCleanupNode> getSetup();

		@Internal
		public abstract DirectoryProperty getProjectDir();

		@TaskAction
		public void npmCiRunTask() throws Exception {
			SetupCleanupNode setup = getSetup().get();
			// install node, npm, and package-lock.json
			setup.start(getProjectDir().get().getAsFile());
			// run the gulp task
			ProxyConfig proxyConfig = new ProxyConfig(Collections.emptyList());
			setup.factory().getNpmRunner(proxyConfig, null).execute("run " + taskName, null);
		}
	}

	@Override
	public void apply(Project project) {
		extension = project.getExtensions().create(EXTENSION_NAME, Extension.class, project);
	}

	private Extension extension;
}