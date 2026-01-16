package us.kbase.kbasegenefamilies.test;

import java.io.*;
import java.util.*;
import java.net.URL;

import org.junit.Test;
import static junit.framework.Assert.*;

import org.strbio.IO;
import org.strbio.util.*;
import com.fasterxml.jackson.databind.*;

import us.kbase.auth.AuthService;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.*;
import us.kbase.workspace.*;
import us.kbase.kbasegenomes.*;
import us.kbase.kbasegenefamilies.*;
import us.kbase.common.taskqueue.TaskQueueConfig;

/**
   Tests for setting up sample db and annotating a self-assembled
   genome locally
*/
public class SelfAssembled2Test {
    private static final String privateWsName = "jmc:gene_domains_test";
    private static final String domainWsName = "KBasePublicGeneDomains";
    private static final String domainModelSetType = "KBaseGeneFamilies.DomainModelSet";
    private static final String domainAnnotationType = "KBaseGeneFamilies.DomainAnnotation";
    private static final String genomeRef = privateWsName+"/genome2";
    private static final String smartRef = domainWsName+"/SMART-only";
    private static final String tigrRef = domainWsName+"/TIGRFAMs-only";

    /**
       check that we can read genome from WS
    */
    @Test public void getGenome() throws Exception {
	Genome genome = null;
	
	System.out.println("Reading genome from WS");
	WorkspaceClient wc = createWsClient(getDevToken());
	genome = wc.getObjects(Arrays.asList(new ObjectIdentity().withRef(genomeRef))).get(0).getData().asClassInstance(Genome.class);

	// save copy for debugging:
	// ObjectMapper mapper = new ObjectMapper();
	// File f = new File("/kb/dev_container/modules/gene_families/data/tmp/sbi");
	// mapper.writeValue(f,genome);
	
	// System.out.println(genome.getScientificName());
	assertEquals(genome.getScientificName(), "Desulfovibrio frigidus DSM 17176");
    }

    /**
       Check that we can get the SMART-only DomainModelSet from the
       public workspace.
    */
    @Test
    public void getSMART() throws Exception {
	WorkspaceClient wc = createWsClient(getDevToken());
	DomainModelSet smart = wc.getObjects(Arrays.asList(new ObjectIdentity().withRef(smartRef))).get(0).getData().asClassInstance(DomainModelSet.class);

	assertEquals(smart.getSetName(),"SMART-only");
    }

    /**
       Check that we can annotate genome with SMART.  This takes less
       than 10 minutes on a 2-CPU Magellan instance.
    */
    @Test
	public void searchGenomePSSM() throws Exception {

	AuthToken token = getDevToken();
	WorkspaceClient wc = createWsClient(token);

	ObjectStorage storage = SearchDomainsBuilder.createDefaultObjectStorage(wc);

	DomainSearchTask dst = new DomainSearchTask(new File("/kb/dev_container/modules/gene_families/data/tmp"), storage);
	
	DomainAnnotation results = dst.runDomainSearch(token.toString(),
						       smartRef,
						       genomeRef);

	wc.saveObjects(new SaveObjectsParams()
		       .withWorkspace(privateWsName)
		       .withObjects(Arrays.asList(new ObjectSaveData()
						  .withType(domainAnnotationType)
						  .withMeta(DomainSearchTask.getMetadata(results))
						  .withName("SMART-genome2")
						  .withData(new UObject(results)))));
    }

    /**
       Check that we can annotate genome with TIGRFAMs.  Takes ~100 min
       on a 2-CPU Magellan instance.
    */
    @Test
	public void searchGenomeHMM() throws Exception {

	AuthToken token = getDevToken();
	WorkspaceClient wc = createWsClient(token);

	ObjectStorage storage = SearchDomainsBuilder.createDefaultObjectStorage(wc);

	DomainSearchTask dst = new DomainSearchTask(new File("/kb/dev_container/modules/gene_families/data/tmp"), storage);
	
	DomainAnnotation results = dst.runDomainSearch(token.toString(),
						       tigrRef,
						       genomeRef);
	wc.saveObjects(new SaveObjectsParams()
		       .withWorkspace(privateWsName)
		       .withObjects(Arrays.asList(new ObjectSaveData()
						  .withType(domainAnnotationType)
						  .withMeta(DomainSearchTask.getMetadata(results))
						  .withName("TIGR-genome2")
						  .withData(new UObject(results)))));
    }

    /**
       creates a workspace client; if token is null, client can
       only read public workspaces
    */
    public static WorkspaceClient createWsClient(AuthToken token) throws Exception {
	WorkspaceClient rv = null;

	TaskQueueConfig cfg = KBaseGeneFamiliesServer.getTaskConfig();
	Map<String,String> props = cfg.getAllConfigProps();
	String wsUrl = props.get(KBaseGeneFamiliesServer.CFG_PROP_WS_SRV_URL);
	if (wsUrl==null)
	    wsUrl = KBaseGeneFamiliesServer.defaultWsUrl;
	
	if (token==null)
	    rv = new WorkspaceClient(new URL(wsUrl));
	else
	    rv = new WorkspaceClient(new URL(wsUrl),token);
	rv.setAuthAllowedForHttp(true);
	return rv;
    }

    /**
       gets the auth token out of the properties file.  To create
       it on your dev instance, do:
       <pre>
       kbase-login (your user name)
       kbase-whoami -t
       </pre>
       Take the resulting text (starting with "un=") and put it in
       the auth.properties file, as auth.token.  Replace the text
       in the file that says "paste token here" with your token.
    */
    public static AuthToken getDevToken() {
	AuthToken rv = null;
	Properties prop = new Properties();
	try {
	    prop.load(EColiTest.class.getClassLoader().getResourceAsStream("auth.properties"));
	}
	catch (IOException e) {
	}
	catch (SecurityException e) {
	}
	try {
	    String value = prop.getProperty("auth.token", null);
	    rv = new AuthToken(value);
	}
	catch (Exception e) {
	    System.out.println(e.getMessage());
	    rv = null;
	}
	return rv;
    }
}