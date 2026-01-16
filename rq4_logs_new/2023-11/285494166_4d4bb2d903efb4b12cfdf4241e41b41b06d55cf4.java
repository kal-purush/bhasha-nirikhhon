package org.recap.camel.submitcollection;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.recap.PropertyKeyConstants;
import org.recap.repository.jpa.InstitutionDetailsRepository;
import org.recap.util.CommonUtil;
import org.recap.util.PropertyUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;

@RunWith(MockitoJUnitRunner.Silent.class)
public class SubmitCollectionPollingS3RouteBuilderTest {

    @InjectMocks
    private SubmitCollectionPollingS3RouteBuilder routeBuilder;

    @Mock
    private ProducerTemplate producer;

    @Mock
    private CamelContext camelContext;

    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private PropertyUtil propertyUtil;

    @Mock
    private InstitutionDetailsRepository institutionDetailsRepository;

    @Mock
    private CommonUtil commonUtil;

    @Mock
    private AmazonS3 awsS3Client;

    @Value("${" + PropertyKeyConstants.SCSB_BUCKET_NAME + "}")
    String scsbBucketName;

    @Value("${" + PropertyKeyConstants.SUBMIT_COLLECTION_LOCAL_DIR + "}")
    String submitCollectionLocalWorkingDir;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        // Mock the properties you need
        ReflectionTestUtils.setField(routeBuilder, "submitCollectionLocalWorkingDir", "submitCollectionLocalWorkingDir + institutionCode + '/cgd_'+ cgdType");
    }

    @Test
    public void testClearDirectory() throws IOException {
        try{
        // Arrange
        String institutionCode = "Institution";
        String cgdType = "yourCgdType";
        File destDirFile = new File(submitCollectionLocalWorkingDir + institutionCode + "/cgd_"+ cgdType);
//        Mockito.doThrow(new IOException("Mocked IOException")).when(FileUtils.class);
        routeBuilder.clearDirectory(institutionCode, cgdType);}catch (Exception e){
            e.printStackTrace();
        }

    }
}