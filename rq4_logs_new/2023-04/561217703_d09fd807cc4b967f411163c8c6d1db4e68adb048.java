package de.tudresden.inf.st.mathgrass.api.feedback.results;

import de.tudresden.inf.st.mathgrass.api.common.AbstractApiElement;
import de.tudresden.inf.st.mathgrass.api.model.TaskResultDTO;
import de.tudresden.inf.st.mathgrass.api.task.TaskRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;

/**
 * API implementation for querying task results. TODO: Implement TaskResultApi interface.
 */
@RestController
public class TaskResultApiImpl extends AbstractApiElement {
    /**
     * Task result repository.
     */
    private final TaskResultRepository taskResultRepository;

    /**
     * Task repository.
     */
    private final TaskRepository taskRepository;

    /**
     * Constructor.
     *
     * @param taskResultRepository task result repository
     * @param taskRepository task repository
     */
    public TaskResultApiImpl(TaskResultRepository taskResultRepository, TaskRepository taskRepository) {
        this.taskResultRepository = taskResultRepository;
        this.taskRepository = taskRepository;
    }

    /**
     * Get all IDs of all task results.
     *
     * @return Response with list of IDs
     */
    public ResponseEntity<List<Long>> getIdsOfAllTaskResults() {
        // find all task results and extract IDs
        List<Long> taskResultIds = taskResultRepository.findAll().stream()
                .map(TaskResult::getId)
                .toList();

        return ok(taskResultIds);
    }

    /**
     * Get a task result by ID.
     *
     * @param taskResultId ID of task result
     * @return Response with task result
     */
    public ResponseEntity<TaskResultDTO> getTaskResultById(Long taskResultId) {
        // find task result by ID in repository
        Optional<TaskResult> optTaskResult = taskResultRepository.findById(taskResultId);
        if (optTaskResult.isEmpty()) {
            return notFound();
        } else {
            // convert to DTO
            TaskResultDTO taskResultDTO = new TaskResultTransformer(taskRepository).toDto(optTaskResult.get());

            return ok(taskResultDTO);
        }
    }
}