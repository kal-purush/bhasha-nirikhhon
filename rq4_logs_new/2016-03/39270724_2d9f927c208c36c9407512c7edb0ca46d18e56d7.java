package runtime.visitors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import runtime.compiler.IErrorContext;
import runtime.helpers.TestHelper;
import runtime.main.CompileMgr;
import runtime.main.CompilerParameters;
import runtime.main.GdlcException;

import static org.junit.Assert.*;

/**
 * Created by jeff on 3/18/16.
 */
public class ResolveRefsVisitorTest extends TestHelper {
    CompilerParameters cp 			= null;
    //	String				TESTDIR		= rootDir + "/gdl/tests";
    // Using relative directories for easier paths, now that GDLC supports them.
    String				INCDIR		= "gdl/tests";
    String				TESTDIR		= "gdl/tests/visitors/resolve_refs_visitor";
    String 				OUTPUTDIR	= "tmp/tests/visitors/resolve_refs_visitor";

    @Before
    public void setUp() throws Exception {
        mkCleanDirs(OUTPUTDIR);
        this.cp = new CompilerParameters();
    }

    @After
    public void tearDown() throws Exception {
        this.cp = null;
    }

    @Test
    public void testMissingVarDefinitionGeneratesHelpfulErrorMessage() throws GdlcException {
        String args[] = {TESTDIR + "/reference_missing_variable.gdl",
                "--I" + TESTDIR,
                "-nooutput",
        };
        String outFile = OUTPUTDIR + "/reference_missing_variable.xml";
        deleteFileIfExists(outFile);

        this.cp.process(args);

        CompileMgr mgr = new CompileMgr();
        try {
            mgr.execute(this.cp);
        } catch (GdlcException e) {
            // Expected error... do nothing.
        }

        IErrorContext ctx = mgr.getContext();
        assertIfContextErrorNotExist(ctx,"Undefined Object Reference. @ rule MissingVarRule:");
        assertIfContextErrorNotExist(ctx,"Variable [missingVar] definition missing.");
    }
}