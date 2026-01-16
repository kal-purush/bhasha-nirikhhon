package org.kpmp.repositoryDataset;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.aspectj.lang.annotation.Before;

public class RepositoryDatasetDisplayTest {
  private RepositoryDatasetDisplay repositoryDatasetDisplay;
  private RepositoryFileDataset repositoryFile;

  @BeforeEach
  public void setUp() throws Exception {
    repositoryDatasetDisplay = new RepositoryDatasetDisplay(repositoryFile);
  }

  @AfterEach
  public void tearDown() throws Exception {
    repositoryDatasetDisplay = null;
  }

  @Test
  public void setDlFileId() {
    repositoryDatasetDisplay.setDlFileId("abc_123");
    assertEquals("abc_123", repositoryDatasetDisplay.getDlFileId());

  }

  @Test
  public void setRedcapId(){
    Set<String> expected = new HashSet<String>(Arrays.asList("redcap_id", "redcap_id2"));
    Set<String> actual = repositoryDatasetDisplay.getRedcapId();
    assertEquals(expected, actual);
  }

  @Test
  public void setSampleType() {
    Set<String> expected = new HashSet<String>(Arrays.asList("sample_type"));
    Set<String> actual = repositoryDatasetDisplay.getSampleType();
    assertEquals(expected, actual);
  }

  @Test
  public void setTissueType() {
    Set<String> expected = new HashSet<String>(Arrays.asList("tissue_type"));
    Set<String> actual = repositoryDatasetDisplay.getTissueType();
    assertEquals(expected, actual);
  }

  @Test
  public void setAgeBinned() {
    Set<String> expected = new HashSet<String>(Arrays.asList("40-49"));
    Set<String> actual = repositoryDatasetDisplay.getAgeBinned();
    assertEquals(expected, actual);
  }

  @Test
  public void setSex() {
    Set<String> expected = new HashSet<String>(Arrays.asList("male"));
    Set<String> actual = repositoryDatasetDisplay.getSex();
    assertEquals(expected, actual);
  }

  @Test
  public void setDoi() {
    Set<String> expected = new HashSet<String>(Arrays.asList("doi"));
    Set<String> actual = repositoryDatasetDisplay.getDoi();
    assertEquals(expected, actual);
  }

  @Test
  public void setAccess(){
    repositoryDatasetDisplay.setAccess("controlled");
    assertEquals("controlled", repositoryDatasetDisplay.getAccess());
  }

  @Test
  public void setPlatform() {
    repositoryDatasetDisplay.setPlatform("platform");
    assertEquals("platform", repositoryDatasetDisplay.getPlatform());
  }

  @Test
  public void setReleaseVersion() {
    repositoryDatasetDisplay.setReleaseVersion("54");
    assertEquals("54", repositoryDatasetDisplay.getReleaseVersion());
  }

  @Test
  public void setProtocol() {
    Set<String> expected = new HashSet<String>(Arrays.asList("Protocol"));
    Set<String> actual = repositoryDatasetDisplay.getProtocol();
    assertEquals(expected, actual);
  }

  @Test
  public void setTissueSource() {
    Set<String> expected = new HashSet<String>(Arrays.asList("kidneys"));
    Set<String> actual = repositoryDatasetDisplay.getTissueSource();
    assertEquals(expected, actual);
  }

  @Test
  public void setExperimentalStrategy() {
    Set<String> expected = new HashSet<String>(Arrays.asList("experimental strategy"));
    Set<String> actual = repositoryDatasetDisplay.getExperimentalStrategy();
    assertEquals(expected, actual);
  }

  @Test
  public void setWorkflowType() {
    Set<String> expected = new HashSet<String>(Arrays.asList("workflowType"));
    Set<String> actual = repositoryDatasetDisplay.getWorkflowType();
    assertEquals(expected, actual);
  }

  @Test
  public void setDataformat() {
    repositoryDatasetDisplay.setDataFormat(".xlsx");
    assertEquals(".xlsx", repositoryDatasetDisplay.getDataFormat());
  }

  @Test
  public void setDataCategory() {
    repositoryDatasetDisplay.setDataCategory("category");
    assertEquals("category", repositoryDatasetDisplay.getDataCategory());
  }

  @Test
  public void setDataType(){
    repositoryDatasetDisplay.setDataType("dataType");
    assertEquals("dataType", repositoryDatasetDisplay.getDataType());
  }

  @Test
  public void setFileSize() {
    Long size = 9L;
    repositoryDatasetDisplay.setFileSize(size);
    assertEquals(9L, repositoryDatasetDisplay.getFileSize());
  }

  @Test
  public void setFileName() {
    repositoryDatasetDisplay.setFileName("FileName");
    assertEquals("FileName", repositoryDatasetDisplay.getFileName());
  }

  @Test
  public void setPackageId(){
    repositoryDatasetDisplay.setPackageId("packageId");
    assertEquals("packageId", repositoryDatasetDisplay.getPackageId());
  }

  @Test
  public void setParticipantIdSort() {
    repositoryDatasetDisplay.getParticipantIdSort();
    assertEquals("Multiple Participants", repositoryDatasetDisplay.getParticipantIdSort());
  }

  @Test
  public void setFileNameSort() {
    repositoryDatasetDisplay.setFileNameSort("this_file_name_has_thirty_seven_chrs_filename.docx");
    assertEquals("filename.docx", repositoryDatasetDisplay.getFileNameSort());
  }

  @Test
  public void setPlatformSort(){
    repositoryDatasetDisplay.getPlatformSort();
    assertEquals("aaaaa", repositoryDatasetDisplay.getPlatformSort());
  }
}
 