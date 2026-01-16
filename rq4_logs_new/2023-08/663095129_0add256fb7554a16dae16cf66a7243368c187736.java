package org.kpmp.repositoryDataset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
public class RepositoryFileDatasetIdTest {

    private RepositoryFileDatasetId repositoryFileDatasetId;
    @BeforeEach
    public void setUp() throws Exception {
        repositoryFileDatasetId = new RepositoryFileDatasetId();
    }

    @AfterEach
    public void tearDown() throws Exception {
        repositoryFileDatasetId = null;
    }

    @Test
    public void setDlFileId() {
        repositoryFileDatasetId.setDlFileId("abc_123");
        assertEquals("abc_123", repositoryFileDatasetId.getDlFileId());
    }

    @Test
    public void setRedcapId() {
        repositoryFileDatasetId.setRedcapId("redcap_id");
        assertEquals("redcap_id", repositoryFileDatasetId.getRedcapId());
    }
}