/*
 * Copyright 2006-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.citrusframework.remote.listener;

import org.citrusframework.TestCase;
import org.citrusframework.TestResult;
import org.citrusframework.remote.model.RemoteResult;
import org.citrusframework.report.OutputStreamReporter;
import org.citrusframework.report.TestListener;
import org.citrusframework.report.TestResults;

import java.io.StringWriter;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Christoph Deppisch
 * @since 2.7.4
 */
public class RemoteTestListener implements TestListener {

    /** Latest test results */
    private TestResults results = new TestResults();

    private final Map<ClassAndName, Long> startTimes = new HashMap<>();

    /**
     * Generate a test report from the current {@link #results}.
     * @return
     */
    public String generateTestReport() {
        StringWriter reportWriter = new StringWriter();
        OutputStreamReporter reporter = new OutputStreamReporter(reportWriter);
        reporter.generate(results);
        return reportWriter.toString();
    }

    /**
     * Obtains the latestResults.
     * @return
     */
    public TestResults getResults() {
        return results;
    }

    @Override
    public void onTestStart(TestCase test) {
        startTimes.put(ClassAndName.of(test), System.currentTimeMillis());
    }

    @Override
    public void onTestFinish(TestCase test) {
        // NOOP
    }

    @Override
    public void onTestSuccess(TestCase test) {
        Duration consumed = Duration.ofMillis(
                System.currentTimeMillis() - startTimes.get(ClassAndName.of(test)));
        results
                .addResult(TestResult.success(
                                test.getName(),
                                test.getTestClass().getCanonicalName(),
                                test.getVariableDefinitions())
                .withDuration(consumed));
    }

    @Override
    public void onTestFailure(TestCase test, Throwable cause) {
        Duration consumed = Duration.ofMillis(
                System.currentTimeMillis() - startTimes.get(ClassAndName.of(test)));
        results
                .addResult(TestResult.failed(
                                test.getName(),
                                test.getTestClass().getCanonicalName(),
                                cause,
                                test.getVariableDefinitions())
                .withDuration(consumed));
    }

    @Override
    public void onTestSkipped(TestCase test) {
        Duration consumed = Duration.ofMillis(
                System.currentTimeMillis() - startTimes.get(ClassAndName.of(test)));
        results
                .addResult(TestResult.skipped(
                        test.getName(),
                        test.getTestClass().getCanonicalName(),
                        test.getVariableDefinitions())
                .withDuration(consumed));
    }

    public List<RemoteResult> toRemoteResults() {
        return getResults().asList().stream()
                .map(RemoteResult::fromTestResult)
                .toList();
    }

    public void reset() {
        results = new TestResults();
        startTimes.clear();
    }

    private record ClassAndName(Class<?> clazz, String name) {
        static ClassAndName of(TestCase testCase) {
            return new ClassAndName(
                    testCase.getTestClass(),
                    testCase.getName());
        }
    }
}