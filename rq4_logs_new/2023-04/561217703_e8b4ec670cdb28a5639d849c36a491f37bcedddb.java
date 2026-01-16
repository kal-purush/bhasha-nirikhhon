package de.tudresden.inf.st.mathgrass.api.task.execution;

import com.google.common.eventbus.EventBus;
import de.tudresden.inf.st.mathgrass.api.events.TaskEvaluationFinishedEvent;
import de.tudresden.inf.st.mathgrass.api.feedback.results.TaskResult;
import de.tudresden.inf.st.mathgrass.api.feedback.results.TaskResultRepository;
import de.tudresden.inf.st.mathgrass.api.task.Task;
import de.tudresden.inf.st.mathgrass.api.task.TaskRepository;
import de.tudresden.inf.st.mathgrass.api.task.question.QuestionVisitor;
import de.tudresden.inf.st.mathgrass.api.task.question.answer.AnswerVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import javax.transaction.Transactional;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Worker class for task execution manager. This class is responsible for the actual evaluation of tasks.
 */
@Component
public class TaskExecutionWorker {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger(TaskExecutionWorker.class);

    /**
     * Task repository.
     */
    private final TaskRepository taskRepository;

    /**
     * Task result repository.
     */
    private final TaskResultRepository taskResultRepository;

    /**
     * Question visitor
     */
    private final QuestionVisitor questionVisitor;

    /**
     * Answer visitor
     */
    private final AnswerVisitor answerVisitor;

    /**
     * Event bus
     */
    private final EventBus eventBus;

    /**
     * Constructor.
     *
     * @param taskRepository task repository
     * @param taskResultRepository task result repository
     * @param questionVisitor question visitor
     * @param answerVisitor answer visitor
     * @param eventBus event bus
     */
    public TaskExecutionWorker(TaskRepository taskRepository, TaskResultRepository taskResultRepository,
            QuestionVisitor questionVisitor, AnswerVisitor answerVisitor, EventBus eventBus) {
        this.taskRepository = taskRepository;
        this.taskResultRepository = taskResultRepository;
        this.questionVisitor = questionVisitor;
        this.answerVisitor = answerVisitor;
        this.eventBus = eventBus;
    }

    @Transactional
    public void runTaskEvaluation(Long taskId, String userAnswer, Long taskResultId) {
        logger.info("Starting task evaluation for task with ID {}", taskId);

        // evaluate answer
        boolean answerCorrect = makeAssessment(taskId, userAnswer);

        // update task result
        updateTaskResult(taskResultId, answerCorrect);

        logger.info("Finished task evaluation for task with ID {}. Task result ID: {}", taskId,
                taskResultId);

        // publish event
        eventBus.post(new TaskEvaluationFinishedEvent(taskResultId));
    }

    /**
     * Evaluate the answer to a certain task.
     *
     * @param taskId ID of task
     * @param userAnswer given answer
     * @return boolean determining whether given answer was correct or not
     * @throws RuntimeException if an error occurs during the evaluation
     */
    public boolean makeAssessment(Long taskId, String userAnswer) throws RuntimeException {
        // get task
        Optional<Task> optTask = taskRepository.findById(taskId);
        if (optTask.isEmpty()) {
            throw new IllegalArgumentException("Couldn't find task with ID " + taskId);
        }

        Task task = optTask.get();
        try {
            return task.getQuestion().acceptQuestionVisitor(questionVisitor, answerVisitor, taskId, userAnswer);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Update the result of a task evaluation.
     *
     * @param taskResultId ID of task result
     * @param answerCorrect boolean determining whether given answer was correct or not
     */
    protected void updateTaskResult(Long taskResultId, boolean answerCorrect) {
        // find task result
        Optional<TaskResult> optTaskResult = taskResultRepository.findById(taskResultId);
        if (optTaskResult.isEmpty()) {
            throw new IllegalArgumentException("Couldn't find task result with ID " + taskResultId);
        }

        TaskResult taskResult = optTaskResult.get();

        // update task result
        taskResult.setAnswerTrue(answerCorrect);
        taskResult.setEvaluationDate(LocalDateTime.now().toString());
        taskResultRepository.save(taskResult);
    }

    /**
     * Create a new task result without evaluation result and date.
     *
     * @param taskId ID of task the result refers to
     * @param userAnswer given answer
     * @return task result
     */
    protected TaskResult createTaskResult(Long taskId, String userAnswer) {
        // find task
        Optional<Task> optTask = taskRepository.findById(taskId);
        if (optTask.isEmpty()) {
            throw new IllegalArgumentException("Couldn't find task with ID " + taskId);
        }

        Task task = optTask.get();

        // create new task result and save
        TaskResult taskResult = new TaskResult();
        taskResult.setTask(task);
        taskResult.setAnswer(userAnswer);
        taskResult.setSubmissionDate(LocalDateTime.now().toString());
        taskResultRepository.save(taskResult);

        return taskResult;
    }
}