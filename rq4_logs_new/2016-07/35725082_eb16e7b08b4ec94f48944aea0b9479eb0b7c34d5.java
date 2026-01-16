package testabstract;

import org.apache.log4j.BasicConfigurator;
import org.ow2.proactive.scripting.ScriptResult;
import org.ow2.proactive.scripting.SimpleScript;
import org.ow2.proactive.scripting.TaskScript;

import java.io.Serializable;

/**
 * Test that the result variable, set from a first script evaluation, is available in a second script evalutation.
 *
 * @author Activeeon Team
 */
public class TestResultMultipleCalls {

    public void test(String engineName) throws Exception {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();
        String rScript = "rm(list = ls()); result=FALSE";

        SimpleScript ss = new SimpleScript(rScript, engineName);
        TaskScript taskScript = new TaskScript(ss);
        ScriptResult<Serializable> res = taskScript.execute();

        System.out.println("Script output:");
        System.out.println(res.getOutput());

        org.junit.Assert
                .assertEquals(
                        "The result variable declared explicitely in the script is not used as the result of the script by the engine",
                        Boolean.FALSE, (Boolean) res.getResult());

        rScript = "rm(list = ls());";

        ss = new SimpleScript(rScript, engineName);
        taskScript = new TaskScript(ss);
        res = taskScript.execute();

        System.out.println("Script output:");
        System.out.println(res.getOutput());

        org.junit.Assert
                .assertEquals(
                        "The result variable from the previous script evaluation should be reused",
                        Boolean.FALSE, (Boolean) res.getResult());

    }
}