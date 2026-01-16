/*
 * Copyright (c) 2021 Cable Television Laboratories, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.siddhi.extension.map.p4;

import com.sun.net.httpserver.HttpServer;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Start Siddhi runtime. Each command line argument must be the full path to the Siddhi script.
 */
public class StartSiddhiRuntime {
    private static final Logger log = Logger.getLogger(StartSiddhiRuntime.class);

    private HttpServer httpServer;
    private final SiddhiManager siddhiManager;
    private final List<String> scriptFiles;
    private final List<SiddhiAppRuntime> runtimes;

    /**
     * Constructor.
     * @param scriptFiles - the list of Siddhi script files to deploy
     */
    public StartSiddhiRuntime(final List<String> scriptFiles) {
        this.httpServer = null;
        this.siddhiManager = new SiddhiManager();
        this.scriptFiles = new ArrayList<>(scriptFiles);
        this.runtimes = new ArrayList<>(scriptFiles.size());
    }

    /**
     * Starts Siddhi runtime and deploys scripts.
     * @throws IOException - when a file cannot be read
     */
    public void start() throws IOException {
        this.httpServer = HttpServer.create(new InetSocketAddress(5005), 0);
        this.httpServer.createContext("/attack", exchange -> log.info(
                "Attack received - " + IOUtils.toString(
                exchange.getRequestBody(), StandardCharsets.UTF_8)));

        for (final String scriptFile : this.scriptFiles) {
            final String script = getFileContents(scriptFile);
            this.runtimes.add(deployScript(script));
        }
        for (final SiddhiAppRuntime runtime : this.runtimes) {
            runtime.start();
        }
    }

    /**
     * Returns the entire contents of a file within a String object.
     * @param filePath - the file's location
     * @return - the file contents
     * @throws IOException - when the file cannot be read
     */
    private static String getFileContents(final String filePath) throws IOException {
        final StringBuilder contentBuilder = new StringBuilder();
        Stream<String> stream = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8);
        stream.forEach(s -> contentBuilder.append(s).append("\n"));
        return contentBuilder.toString();
    }

    /**
     * Stops all resources.
     */
    public void stop() {
        for (final SiddhiAppRuntime runtime : this.runtimes) {
            runtime.shutdown();
        }
        siddhiManager.shutdown();
        this.httpServer.stop(0);
    }

    /**
     * Deploys the Siddhi script to the manager.
     * @param script - the Siddhi script
     * @return - the created runtime object
     */
    private SiddhiAppRuntime deployScript(final String script) {
        log.info("Deploying script to Siddhi - " + script);
        return this.siddhiManager.createSiddhiAppRuntime(script);
    }

    /**
     * Start Siddhi and runtimes.
     * @param args - array of Siddhi script files
     * @throws Exception - when something bad happens
     */
    public static void main(String[] args) throws Exception {
        log.info("Starting Siddhi Runtime with args length " + args.length);
        for (final String arg : args) {
            log.info("with arg - " + arg);
        }
        final StartSiddhiRuntime starter = new StartSiddhiRuntime(Arrays.asList(args));

        // Add SIGTERM shutdown hook for cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Inside Add Shutdown Hook");
            starter.stop();
        }));

        log.info("Shut Down Hook Attached now start Siddhi.");
        starter.start();
    }
}