package org.shikshalokam.backend.ep;

import io.restassured.response.Response;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.shikshalokam.backend.PropertyLoader;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;

public class TestElevateBulkEntityTypeCRUDOperations extends ElevateProjectBaseTest {
    private static final Logger logger = LogManager.getLogger(TestElevateBulkEntityTypeCRUDOperations.class);
    private List<String> randomEntityTypeName = new ArrayList<>();

    @BeforeMethod
    public void userLogin() {
        loginToElevate(PropertyLoader.PROP_LIST.getProperty("elevate.login.contentcreator"), PropertyLoader.PROP_LIST.getProperty("elevate.login.contentcreator.password"));
    }

    @Test(description = "Test case to create entity using valid CSV")
    public void testBulkCreateEntityTypeWithValidCsv() {
        updateCsvWithName("src/main/resources/bulk_entity_type_create.csv", "target/classes/bulk_entity_type_create.csv");
        Response response = testUploadCsvFile("target/classes/bulk_entity_type_create.csv", "elevate.qa.bulkenitytype.create.endpoint");
        Assert.assertEquals(response.getStatusCode(), 200);
        Assert.assertTrue(response.asString().contains("SUCCESS"));
        logger.info("Validations related to valid CSV bulk upload entity file is verified");
    }

    @Test(description = "Test case to create entity using Invalid CSV")
    //This negative scenario cannot be verified at the moment because of the below issue
    //https://katha.shikshalokam.org/bug-view-1971.html
    public void testBulkCreateEntityTypeWithInvalidCsv() {
        Response response = testUploadCsvFile("src/main/resources/bulk_entity_type_invalid_Elevate.csv", "elevate.qa.bulkenitytype.create.endpoint");
        Assert.assertEquals(response.getStatusCode(), 400);
        //Assertion statement to be added once the issue is fixed
        logger.info("Validations related to Invalid CSV bulk upload entity file is verified");
    }

    @Test(description = "Test case to update the entity using valid CSV", dependsOnMethods = "testBulkCreateEntityTypeWithValidCsv")
    public void testBulkUpdateEntityTypeWithValidCsv() {
        updateCsvWithID("src/main/resources/bulk_entity_type_update_operations.csv", "target/classes/bulk_entity_type_update_operations.csv");
        updateCsvWithName("target/classes/bulk_entity_type_update_operations.csv", "target/classes/bulk_entity_type_update_operations.csv");
        Response response = testUploadCsvFile("target/classes/bulk_entity_type_update_operations.csv", "elevate.qa.bulkenitytype.update.endpoint");
        Assert.assertEquals(response.getStatusCode(), 200);
        Assert.assertTrue(response.asString().contains("SUCCESS"));
        logger.info("Validations related to valid CSV bulk update entity file is verified");
    }

    @Test(description = "Test case to update the entity using Invalid CSV")
    //This negative scenario cannot be verified at the moment because of the below issue
    //https://katha.shikshalokam.org/bug-view-1971.html
    public void testBulkUpdateEntityTypeWithInvalidCsv() {
        Response response = testUploadCsvFile("src/main/resources/bulk_entity_type_invalid_Elevate.csv", "elevate.qa.bulkenitytype.update.endpoint");
        Assert.assertEquals(response.getStatusCode(), 400);
        logger.info("Validations related to Invalid CSV bulk update entity file is verified");
    }

    private Response testUploadCsvFile(String path, String endpoint) {
        // Path to the CSV file
        File csvFile = new File(path);
        if (!csvFile.exists()) {
            logger.error("CSV file does not exist: " + csvFile.getAbsolutePath());
        }
        // Send the POST request to upload the CSV file
        Response response = given().multiPart("entityTypes", csvFile)
                .header("internal-access-token", INTERNAL_ACCESS_TOKEN)  // Internal access token header
                .header("x-auth-token", X_AUTH_TOKEN)  // x-authenticated-token header
                .header("Content-Type", "multipart/form-data")  // Content-Type for file upload
                .post(PropertyLoader.PROP_LIST.getProperty(endpoint));
        logger.info("Response Status Code: " + response.getStatusCode());
        response.prettyPrint();
        return response;
    }

    private void updateCsvWithName(String sourcePath, String targetPath) {
        try {
            String CSVcontent = new String(Files.readAllBytes(Paths.get(sourcePath)));
            logger.info("Original Content:\n" + CSVcontent);
            randomEntityTypeName = Stream.generate(() -> RandomStringUtils.randomAlphabetic(10))
                    .limit(3)
                    .collect(Collectors.toList());
            for (String EntityTypeName : randomEntityTypeName) {
                CSVcontent = CSVcontent.replaceFirst("EntityTypeName", EntityTypeName);
            }
            Files.write(Paths.get(targetPath), CSVcontent.getBytes());
            logger.info("Updated Content:\n" + CSVcontent);
        } catch (IOException e) {
            logger.info(e + "Error!!!");
        }
    }

    private void updateCsvWithID(String sourcePath, String targetPath) {
        try {
            String CSVcontent = new String(Files.readAllBytes(Paths.get(sourcePath)));
            HashMap<String, String> map = new HashMap<>();
            List<String> entitytypeId = new ArrayList<>();
            for (String name : randomEntityTypeName) {
                String entityType_Id = getEntitytype_Id(name);
                map.put(name, entityType_Id);
                entitytypeId.add(entityType_Id);
            }
            System.out.println("keylist = " + entitytypeId);
            for (String EntitytypeID : entitytypeId) {
                CSVcontent = CSVcontent.replaceFirst("EntitytypeID", EntitytypeID);
            }
            Files.write(Paths.get(targetPath), CSVcontent.getBytes());
            logger.info("Updated Content:\n" + CSVcontent);

        } catch (
                IOException e) {
            logger.info(e + "Error!!!");
        }
    }
}