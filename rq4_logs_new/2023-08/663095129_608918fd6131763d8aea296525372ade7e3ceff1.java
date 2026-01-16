package org.kpmp;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kpmp.repositoryDataset.RepositoryDatasetDisplay;
import org.kpmp.repositoryDataset.RepositoryDatasetService;
import org.kpmp.repositoryDataset.RepositoryFileDataset;
import org.kpmp.repositoryDataset.RepositoryFileDatasetId;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class QueryTest {
    @Mock
    private RepositoryDatasetService repositoryDatasetService;
    @Mock
    private RepositoryDatasetDisplay repositoryDatasetDisplay;
    private Query query;
    
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        query = new Query(repositoryDatasetService);
    }

    @After
    public void tearDown() throws Exception{
        MockitoAnnotations.openMocks(this).close();
        query = null;
    }

    @Test
    public void testGetRepositoryDataset() throws Exception { 
        RepositoryFileDataset repositoryFileDataset = new RepositoryFileDataset();
        RepositoryFileDatasetId repositoryFileDatasetId = new RepositoryFileDatasetId();
        Long size = 9L;
        repositoryFileDataset.setAccess("controlled");
        repositoryFileDataset.setAgeBinned("45-50");
        repositoryFileDataset.setDataCategory("data_category");
        repositoryFileDataset.setDataFormat(".xlsx");
        repositoryFileDataset.setDataType("dataType");
        repositoryFileDataset.setDoi("doi");
        repositoryFileDataset.setExperimentalStrategy("strategy");
        repositoryFileDataset.setFileName("Filename.xlsx");
        repositoryFileDataset.setFileSize(size);
        repositoryFileDataset.setPackageId("package_id");
        repositoryFileDataset.setPlatform("platform");
        repositoryFileDataset.setProtocol("protocol");
        repositoryFileDataset.setReleaseVersion("9");
        repositoryFileDataset.setSampleType("SampleType");
        repositoryFileDataset.setSex("Male");
        repositoryFileDataset.setTissueSource("Kidneys");
        repositoryFileDataset.setTissueType("organic");
        repositoryFileDataset.setWorkflowType("workflowType");
        repositoryFileDatasetId.setDlFileId("dlFileId");
        repositoryFileDataset.setId(repositoryFileDatasetId);
        
        List<RepositoryDatasetDisplay> expectedResult = Arrays.asList(new RepositoryDatasetDisplay(repositoryFileDataset));
        when(repositoryDatasetService.getRepositoryDataset()).thenReturn(expectedResult);

        assertEquals(expectedResult, repositoryDatasetService.getRepositoryDataset());
    }

}