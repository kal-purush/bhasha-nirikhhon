package br.lassal.dbvcs.tatubola.integration.versioncontrol;

import br.lassal.dbvcs.tatubola.builder.RelationalDBVersionFactory;
import br.lassal.dbvcs.tatubola.integration.IntegrationTestInfo;
import br.lassal.dbvcs.tatubola.integration.util.FileSystemUtil;
import br.lassal.dbvcs.tatubola.versioncontrol.GitController;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystem;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystemException;
import junit.framework.TestCase;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GitControllerTest {

    private static Logger logger = LoggerFactory.getLogger(GitControllerTest.class);

    private final File rootWorkspace = new File("gitControllerTests");

    public GitControllerTest(){
        this.cleanUpWorkspace();
    }

    private void cleanUpWorkspace(){
        FileSystemUtil.deleteDir(this.rootWorkspace);
    }

    private GitController vcs;

    /**
     * Creates a VCS Controller using the following ENV variables
     *   - GITREPO_USER : username in GitHub for the repository
     *   - GITREPO_PWD : password for the GitHub account
     * @return
     */
    private GitController getVCSController(){
        if(this.vcs == null){
            try {
                URL githubRepo = new URL(IntegrationTestInfo.REMOTE_REPO);
                String username = IntegrationTestInfo.getVCSRepositoryUsername();
                String password = IntegrationTestInfo.getVCSRepositoryPassword();
                String baseBranch = IntegrationTestInfo.REPO_BASE_BRANCH;

                this.vcs = (GitController) RelationalDBVersionFactory.getInstance()
                        .createVCSController(IntegrationTestInfo.REMOTE_REPO, username, password, baseBranch);
                this.vcs.setWorkspacePath(this.rootWorkspace);

            } catch (MalformedURLException e) {
                logger.error("Error trying to create GitController in test", e);
            }
        }

        return this.vcs;
    }


    private String getGitHead() throws IOException {
        String currentGitHead = new String(Files.readAllBytes(Paths.get(this.rootWorkspace.getAbsolutePath(),".git/HEAD")));

        return currentGitHead.trim();
    }

    /**
     * Tests the setup of the local repository using the test credentials
     *  - Creates a new local Git repository in the rootWorkspace
     *  - Checks if git creates the base infrastructure
     *
     * @throws VersionControlSystemException
     * @throws IOException
     */
    @Test
    public void test1_setupLocalRepository() throws VersionControlSystemException, IOException {
        GitController vcs = this.getVCSController();

        //first test verify workspace does not exists
        assertFalse(this.rootWorkspace.exists());

        vcs.setupRepositoryInitialState();
        assertTrue(this.rootWorkspace.exists());

        String currentGitHead = this.getGitHead();
        String expectedHead = "ref: refs/heads/master";

        assertEquals(expectedHead, currentGitHead);
    }

    /**
     * Test the checkout of a new branch and commit in the local git repository in
     * the rootWorkspace
     * @throws VersionControlSystemException
     * @throws IOException
     */
    @Test
    public void test2_changeBranch() throws VersionControlSystemException, IOException {
        GitController vcs = this.getVCSController();
        vcs.setupRepositoryInitialState();
        String branch = "DummyBranch";

        vcs.checkout(branch);
        String currentGitHead = this.getGitHead();
        String expectedHead = "ref: refs/heads/" + branch;

        assertEquals(expectedHead, currentGitHead);

        String sampleContent = "Line 001 \n Line 002 \n";
        Path outputSampleFile = Paths.get(this.rootWorkspace.getAbsolutePath(), "samplefile.txt");
        Files.write(outputSampleFile, sampleContent.getBytes());

        vcs.commitAllChanges("Sample commit");
    }

    /**
     * Try to sync the local changes (after test1 and test2) to the remote server using
     * invalid credentials; that way an exception is expected
     * @throws VersionControlSystemException
     */
    @Test(expected = VersionControlSystemException.class)
    public void test3_trySyncServerInvalidCredentials() throws VersionControlSystemException {
        GitController vcs = null;

        try{
            URL githubRepo = new URL(IntegrationTestInfo.REMOTE_REPO);
            String username = "nonono";
            String password = "nonono";
            String baseBranch = "DummyBranch";

            vcs = (GitController) RelationalDBVersionFactory.getInstance()
                    .createVCSController(IntegrationTestInfo.REMOTE_REPO, username, password, baseBranch);
            vcs.setWorkspacePath(this.rootWorkspace);
            vcs.setupRepositoryInitialState();

        }catch (Exception ex){
            logger.error("Error during test of Sync server with wrong credentials", ex);
        }

        vcs.syncChangesToServer();

    }
}