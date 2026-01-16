/*
 * ProActive Parallel Suite(TM):
 * The Open Source library for parallel and distributed
 * Workflows & Scheduling, Orchestration, Cloud Automation
 * and Big Data Analysis on Enterprise Grids & Clouds.
 *
 * Copyright (c) 2007 - 2017 ActiveEon
 * Contact: contact@activeeon.com
 *
 * This library is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation: version 3 of
 * the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * If needed, contact us to obtain a release under GPL Version 2 or 3
 * or a different license than the AGPL.
 */
package jsr223.docker.file;

import static org.junit.Assume.assumeTrue;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ow2.proactive.scheduler.common.SchedulerConstants;
import org.ow2.proactive.scheduler.task.SchedulerVars;
import org.ow2.proactive.scripting.ScriptResult;
import org.ow2.proactive.scripting.SimpleScript;
import org.ow2.proactive.scripting.TaskScript;

import com.google.common.io.Files;


public class DockerFileScriptEngineTest {

    private boolean dockerCommandExists() {
        try {
            Process p = Runtime.getRuntime().exec("docker -v");
            p.waitFor();
            return p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @BeforeClass
    public static void before() {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();
        DockerFilePropertyLoader.getInstance().setDockerFileCommand("docker");
        DockerFilePropertyLoader.getInstance().setUseSudo(false);
        DockerFilePropertyLoader.getInstance().setDockerHost("");
    }

    @Test
    public void testContainerAndImageShouldUseJobTaskIds() throws Exception {
        assumeTrue(dockerCommandExists());

        String dockerScript = "FROM ubuntu:18.04\n" + "RUN echo \"Hello\"";

        SimpleScript ss = new SimpleScript(dockerScript, DockerFileScriptEngineFactory.NAME);
        TaskScript taskScript = new TaskScript(ss);
        HashMap<String, Serializable> variablesMap = new HashMap<String, Serializable>(2);
        variablesMap.put(SchedulerVars.PA_JOB_ID.name(), "1");
        variablesMap.put(SchedulerVars.PA_TASK_ID.name(), "2");
        Map<String, Object> aBindings = Collections.singletonMap(SchedulerConstants.VARIABLES_BINDING_NAME,
                                                                 (Object) variablesMap);

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        ScriptResult<Serializable> res = taskScript.execute(aBindings,
                                                            new PrintStream(output),
                                                            new PrintStream(output));

        System.out.println("Script output:");
        System.out.println(output.toString());

        System.out.println("Script Exception:");
        System.out.println(res.getException());

        Assert.assertFalse(res.errorOccured());

        Assert.assertTrue("Output should contain the dedicated image name",
                          output.toString().contains(DockerFileScriptEngine.DEFAULT_IMAGE_NAME + "_1t2"));
        Assert.assertTrue("Output should contain the dedicated container name",
                          output.toString().contains(DockerFileScriptEngine.DEFAULT_CONTAINER_NAME + "_1t2"));
    }

    @Test
    public void testDockerFileShouldBeCreatedInsideScratchSpace() throws Exception {
        assumeTrue(dockerCommandExists());
        String dockerScript = "FROM ubuntu:18.04\n" + "RUN echo \"Hello\"";

        DockerFilePropertyLoader.getInstance().setKeepDockerFile(true);

        File tempDir = Files.createTempDir();

        SimpleScript ss = new SimpleScript(dockerScript, DockerFileScriptEngineFactory.NAME);
        TaskScript taskScript = new TaskScript(ss);

        Map<String, Object> aBindings = Collections.singletonMap(SchedulerConstants.DS_SCRATCH_BINDING_NAME,
                                                                 (Object) tempDir.getAbsolutePath());

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        ScriptResult<Serializable> res = taskScript.execute(aBindings,
                                                            new PrintStream(output),
                                                            new PrintStream(output));

        System.out.println("Script output:");
        System.out.println(output.toString());

        System.out.println("Script Exception:");
        System.out.println(res.getException());

        Assert.assertFalse(res.errorOccured());

        Assert.assertTrue("Docker file should be created in temp directory",
                          new File(tempDir, DockerFileCommandCreator.FILENAME).exists());
    }

    @Test
    public void testContainerOutputShouldBeRetrieved() throws Exception {
        assumeTrue(dockerCommandExists());
        String capturedOutput = "Hello from " + this.getClass().getName();
        String dockerScript = "FROM ubuntu:18.04\n" + "RUN echo \"" + capturedOutput + "\"";

        SimpleScript ss = new SimpleScript(dockerScript, DockerFileScriptEngineFactory.NAME);
        TaskScript taskScript = new TaskScript(ss);

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        ScriptResult<Serializable> res = taskScript.execute(null, new PrintStream(output), new PrintStream(output));

        System.out.println("Script output:");
        System.out.println(output.toString());

        System.out.println("Script Exception:");
        System.out.println(res.getException());

        Assert.assertFalse(res.errorOccured());

        Assert.assertTrue("Captured output should appear", output.toString().contains(capturedOutput));
    }
}