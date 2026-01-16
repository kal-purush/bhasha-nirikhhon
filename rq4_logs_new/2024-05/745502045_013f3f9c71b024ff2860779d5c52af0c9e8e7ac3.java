package data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.cloud.dataflow.schema.AppBootSchemaVersion.BOOT3;

import data.config.MetisDataflowClientConfig;
import data.config.properties.BatchConfigurationProperties;
import data.config.properties.RegisterConfigurationProperties;
import java.lang.invoke.MethodHandles;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.dataflow.core.ApplicationType;
import org.springframework.cloud.dataflow.rest.client.DataFlowOperations;
import org.springframework.cloud.dataflow.rest.resource.AppRegistrationResource;
import org.springframework.cloud.dataflow.rest.resource.DetailedAppRegistrationResource;
import org.springframework.cloud.dataflow.rest.resource.TaskDefinitionResource;
import org.springframework.cloud.dataflow.rest.resource.TaskExecutionResource;
import org.springframework.hateoas.PagedModel;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(classes = {MetisDataflowClientConfig.class})
@EnableAutoConfiguration
class RegistrationTestIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  @Autowired
  DataFlowOperations dataFlowOperations;
  @Autowired
  BatchConfigurationProperties batchConfigurationProperties;

  @Test
  void registerApplications() {
    final RegisterConfigurationProperties registerProperties = batchConfigurationProperties.getRegisterProperties();
    registerApplication(registerProperties.getOaiHarvestName(), registerProperties.getOaiHarvestUri());
    registerApplication(registerProperties.getValidationName(), registerProperties.getValidationUri());
    registerApplication(registerProperties.getTransformationName(), registerProperties.getTransformationUri());
    registerApplication(registerProperties.getNormalizationName(), registerProperties.getNormalizationUri());
    registerApplication(registerProperties.getEnrichmentName(), registerProperties.getEnrichmentUri());
    registerApplication(registerProperties.getMediaName(), registerProperties.getMediaUri());
  }

  private void registerApplication(String name, String uri) {
    dataFlowOperations.appRegistryOperations()
                      .register(name, ApplicationType.task, uri, "", BOOT3, true);
    final DetailedAppRegistrationResource detailedAppRegistrationResource =
        dataFlowOperations.appRegistryOperations().info(name, ApplicationType.task, false);
    assertEquals(name, detailedAppRegistrationResource.getName());
  }

  @Test
  void unregisterApplications() {
    dataFlowOperations.appRegistryOperations().unregisterAll();
    assertEquals(0, dataFlowOperations.appRegistryOperations().list().getContent().size());
  }

  @Test
  void createTasks() {
    final RegisterConfigurationProperties registerProperties = batchConfigurationProperties.getRegisterProperties();
    final PagedModel<TaskDefinitionResource> taskDefinitionResources = dataFlowOperations.taskOperations().list();
    createTask(taskDefinitionResources, registerProperties.getOaiHarvestName(), registerProperties.getOaiHarvestName());
    createTask(taskDefinitionResources, registerProperties.getValidationName(), registerProperties.getValidationName());
    createTask(taskDefinitionResources, registerProperties.getTransformationName(), registerProperties.getTransformationName());
    createTask(taskDefinitionResources, registerProperties.getNormalizationName(), registerProperties.getNormalizationName());
    createTask(taskDefinitionResources, registerProperties.getEnrichmentName(), registerProperties.getEnrichmentName());
    createTask(taskDefinitionResources, registerProperties.getMediaName(), registerProperties.getMediaName());
  }

  @Test
  void destroyTasks() {
    dataFlowOperations.taskOperations().destroyAll();
    final PagedModel<TaskExecutionResource> taskExecutionResources = dataFlowOperations.taskOperations().executionList();
    for (TaskExecutionResource taskExecutionResource : taskExecutionResources) {
      dataFlowOperations.taskOperations().cleanupAllTaskExecutions(false, taskExecutionResource.getTaskName());
    }
    assertEquals(0, dataFlowOperations.taskOperations().list().getContent().size());
  }

  private void createTask(PagedModel<TaskDefinitionResource> taskDefinitionResources, String name, String definition) {
    //Filter already existing
    for (TaskDefinitionResource taskDefinitionResource : taskDefinitionResources) {
      if (taskDefinitionResource.getName().equals(name)) {
        assertEquals(name, taskDefinitionResource.getName());
        return;
      }
    }
    final TaskDefinitionResource taskDefinitionResource = dataFlowOperations.taskOperations().create(name, definition, "");
    assertEquals(name, taskDefinitionResource.getName());
  }

  @Test
  void logPresentObjects() {
    final PagedModel<AppRegistrationResource> appRegistrationResources = dataFlowOperations.appRegistryOperations().list();

    LOGGER.info("All Registered Applications:");
    for (AppRegistrationResource appRegistration : appRegistrationResources) {
      LOGGER.info("Application name: {}", appRegistration.getName());
      LOGGER.info("Type: {}", appRegistration.getType());
      LOGGER.info("URI: {}", appRegistration.getUri());
      LOGGER.info("");
    }

    LOGGER.info("All Created Tasks:");
    final PagedModel<TaskDefinitionResource> taskDefinitionResources = dataFlowOperations.taskOperations().list();
    for (TaskDefinitionResource taskDefinitionResource : taskDefinitionResources) {
      LOGGER.info("Task name: {}", taskDefinitionResource.getName());
      LOGGER.info("Description: {}", taskDefinitionResource.getDescription());
      LOGGER.info("DSL: {}", taskDefinitionResource.getDslText());
      LOGGER.info("");
    }
    assertTrue(true);
  }
}