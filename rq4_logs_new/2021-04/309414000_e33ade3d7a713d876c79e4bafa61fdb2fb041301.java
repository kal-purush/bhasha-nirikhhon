package uk.gov.hmcts.reform.wastandalonetaskbpmn;

import org.camunda.bpm.engine.ManagementService;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.TaskService;
import org.camunda.bpm.engine.impl.cfg.StandaloneInMemProcessEngineConfiguration;
import org.camunda.bpm.engine.runtime.JobQuery;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.task.Task;
import org.camunda.bpm.engine.test.ProcessEngineRule;
import org.camunda.bpm.engine.test.assertions.bpmn.BpmnAwareTests;
import org.junit.Before;
import org.junit.Rule;

import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.time.ZonedDateTime.now;
import static org.camunda.bpm.engine.test.assertions.bpmn.BpmnAwareTests.externalTask;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class CamundaProcessEngineBaseUnitTest {
    public static final String TEST_BUSINESS_KEY = "aBusinessKey";
    public static final String DATE_TIME_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";
    private static final ProcessEngine INSTANCE = new StandaloneInMemProcessEngineConfiguration().buildProcessEngine();
    public final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_TIME_FORMAT_PATTERN);

    @Rule
    public ProcessEngineRule processEngineRule = new ProcessEngineRule(INSTANCE);

    public ManagementService managementService;

    @Before
    public void setUp() {
        managementService = processEngineRule.getManagementService();
    }

    public ProcessInstance startCreateTaskProcessWithBusinessKey(Map<String, Object> processVariables,
                                                                 String businessKey) {
        return processEngineRule.getRuntimeService()
            .startProcessInstanceByMessage("createTaskMessage", businessKey, processVariables);
    }

    /**
     * Helper method that creates a process instance and progresses it up to the processTask stage
     *
     * @param withDelayUntil whether the task should be created with a delay
     * @return The resulting process instance at processTask stage
     */
    public ProcessInstance createTask(boolean withDelayUntil) {

        String delayUntilValue = now().plusSeconds(1).format(DateTimeFormatter.ofPattern(DATE_TIME_FORMAT_PATTERN));

        ZonedDateTime dueDate = now().plusDays(7);

        Map<String, Object> variables = new HashMap<>();
        variables.put("taskId", "provideRespondentEvidence");
        variables.put("group", "TCW");
        variables.put("dueDate", dueDate.format(DateTimeFormatter.ofPattern(DATE_TIME_FORMAT_PATTERN)));
        variables.put("name", "Provide respondent evidence");
        variables.put("taskState", "configured");
        variables.put("taskCategory", "Case Progression");
        variables.put("location", "765324");
        variables.put("locationName", "Taylor House");
        variables.put("caseId", "012345678");
        variables.put("jurisdiction", "IA");
        variables.put("caseTypeId", "Asylum");
        variables.put("workingDaysAllowed", 2);
        variables.put("isDuplicate", false);

        if (withDelayUntil) {
            variables.put("delayUntil", delayUntilValue);
        }

        ProcessInstance processInstance = startCreateTaskProcessWithBusinessKey(variables, TEST_BUSINESS_KEY);

        //Check task is waiting at idempotency check and manually execute
        BpmnAwareTests.assertThat(processInstance).isWaitingAt("idempotencyCheck");
        BpmnAwareTests.complete(externalTask());

        //Check task is waiting at timer check and manually execute
        BpmnAwareTests.assertThat(processInstance).isWaitingAt("processStartTimer");
        JobQuery jobQuery = managementService.createJobQuery().processInstanceId(processInstance.getId());

        final Date processDueDate = jobQuery.singleResult().getDuedate();

        if (withDelayUntil) {
            assertEquals(delayUntilValue, dateFormat.format(processDueDate));
        } else {
            //When DelayUntil is not set a default due date is added
            assertEquals("2000-01-01T00:00:00", dateFormat.format(processDueDate));
        }

        managementService.executeJob(jobQuery.singleResult().getId());

        //Check task is at processTask and assert properties
        BpmnAwareTests.assertThat(processInstance).isWaitingAt("processTask");
        BpmnAwareTests.assertThat(processInstance).isStarted()
            .task()
            .hasDefinitionKey("processTask")
            .hasCandidateGroup("TCW")
            .hasName("Provide respondent evidence")
            .isNotAssigned();

        TaskService taskService = processEngineRule.getTaskService();
        Task task = taskService.createTaskQuery().processInstanceId(processInstance.getId()).singleResult();

        //Retrieve all tasks variables
        Map<String, Object> taskVariables = taskService.getVariables(task.getId());
        //LocalVariables
        assertTrue(taskVariables.containsKey("name"));
        assertTrue(taskVariables.containsKey("taskType"));
        assertTrue(taskVariables.containsKey("taskState"));
        assertTrue(taskVariables.containsKey("taskCategory"));
        assertTrue(taskVariables.containsKey("location"));
        assertTrue(taskVariables.containsKey("locationName"));
        assertTrue(taskVariables.containsKey("caseId"));
        assertTrue(taskVariables.containsKey("jurisdiction"));
        assertTrue(taskVariables.containsKey("caseTypeId"));
        assertTrue(taskVariables.containsKey("workingDaysAllowed"));
        //ProcessVariables
        assertTrue(taskVariables.containsKey("taskId"));
        assertTrue(taskVariables.containsKey("dueDate"));
        assertTrue(taskVariables.containsKey("isDuplicate"));
        assertTrue(taskVariables.containsKey("group"));

        //If something else gets added this should fail and act as a safe-guard
        if (withDelayUntil) {
            assertTrue(taskVariables.containsKey("delayUntil"));
            assertEquals(15, taskVariables.size());
        } else {
            assertEquals(14, taskVariables.size());
        }

        return processInstance;
    }

}