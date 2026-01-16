package de.tudresden.inf.st.mathgrass.api.feedback.results;

import de.tudresden.inf.st.mathgrass.api.model.TaskResultDTO;
import de.tudresden.inf.st.mathgrass.api.task.Task;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Test class for {@link TaskResultApiImpl}.
 */
@SpringBootTest
@ActiveProfiles(profiles = "test")
class TaskResultApiImplTest {
    /**
     * Task result API.
     */
    @Autowired
    private TaskResultApiImpl taskResultApi;

    /**
     * Task result repository.
     */
    @MockBean
    private TaskResultRepository taskResultRepository;

    /**
     * Test that the correct number of task result IDs is returned when list of IDs is requested.
     */
    @Test
    void correctNumberOfTaskResultIds() {
        // create expected list of task result IDs
        List<Long> expectedTaskResultIds = List.of(1L, 2L, 3L);

        // mock task result repository
        when(taskResultRepository.findAll()).thenReturn(expectedTaskResultIds.stream()
                .map(id -> {
                    TaskResult taskResult = new TaskResult();
                    taskResult.setId(id);
                    return taskResult;
                })
                .toList());

        // get list of task result IDs
        List<Long> taskResultIds = taskResultApi.getIdsOfAllTaskResults().getBody();

        assert taskResultIds != null;

        // check that the correct number of IDs is returned
        assertEquals(expectedTaskResultIds, taskResultIds);
    }

    /**
     * Test that the correct task result is returned when a task result ID is requested.
     */
    @Test
    void correctTaskResult() {
        // create expected task result (necessary fields only to convert to DTO)
        TaskResult taskResult = new TaskResult();
        taskResult.setTask(new Task());
        taskResult.setSubmissionDate("test");
        taskResult.setEvaluationDate("test");
        taskResult.setAnswer("test");
        taskResult.setAnswerTrue(true);

        // mock task result repository
        when(taskResultRepository.findById(taskResult.getId())).thenReturn(java.util.Optional.of(taskResult));

        // get task result
        TaskResultDTO taskResultDTO = taskResultApi.getTaskResultById(taskResult.getId()).getBody();

        assert taskResultDTO != null;

        // check that the correct task result is returned
        assertEquals(taskResult.getId(), taskResultDTO.getId());
    }
}