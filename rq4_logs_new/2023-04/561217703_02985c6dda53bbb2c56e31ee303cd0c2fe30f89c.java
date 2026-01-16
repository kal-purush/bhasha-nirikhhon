package de.tudresden.inf.st.mathgrass.api.demodata;

import de.tudresden.inf.st.mathgrass.api.evaluator.evaluators.sage.SageEvaluatorPlugin;
import de.tudresden.inf.st.mathgrass.api.graph.Edge;
import de.tudresden.inf.st.mathgrass.api.graph.Graph;
import de.tudresden.inf.st.mathgrass.api.graph.GraphRepository;
import de.tudresden.inf.st.mathgrass.api.graph.Vertex;
import de.tudresden.inf.st.mathgrass.api.task.Task;
import de.tudresden.inf.st.mathgrass.api.task.TaskRepository;
import de.tudresden.inf.st.mathgrass.api.task.hint.Hint;
import de.tudresden.inf.st.mathgrass.api.task.question.FormQuestion;
import de.tudresden.inf.st.mathgrass.api.task.question.answer.StaticAnswer;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * This class fills up the repositories with generated demo data.
 */
@Profile("demodata")
@Component
public class SageTaskDemoDataProvider {

    private final GraphRepository graphRepo;
    private final TaskRepository taskRepo;

    /**
     * Constructor.
     *
     * @param graphRepo graph repository
     * @param taskRepo  task repository
     */
    public SageTaskDemoDataProvider(GraphRepository graphRepo, TaskRepository taskRepo, SageEvaluatorPlugin sagePlugin) {
        this.graphRepo = graphRepo;
        this.taskRepo = taskRepo;
    }

    /**
     * Initialize graphs.
     */
    @PostConstruct
    private void initTasks() {
        createStaticTask();
    }

    /**
     * Generate a task with static evaluation.
     */
    private void createStaticTask() {
        // create graph
        Graph graph = new Graph();

        // create vertices
        Vertex vertex1 = new Vertex();
        final String LABEL_SOURCE = "1";
        vertex1.setLabel(LABEL_SOURCE);
        vertex1.setX(20);
        vertex1.setY(20);

        Vertex vertex2 = new Vertex();
        vertex2.setLabel("2");
        vertex2.setX(60);
        vertex2.setY(60);

        // create edges
        Edge edge1 = new Edge();
        edge1.setSourceVertex(vertex1);
        edge1.setTargetVertex(vertex2);

        // add vertices and edges to graph
        graph.setVertices(List.of(vertex1, vertex2));
        graph.setEdges(List.of(edge1));
        graphRepo.save(graph);

        // create task
        Task demoTask1 = new Task();
        demoTask1.setGraph(graph);
        demoTask1.setLabel("Task with simple evaluation");

        FormQuestion question = new FormQuestion();
        question.setQuestionText("What's the label of the source vertex?");

        StaticAnswer answer = new StaticAnswer();
        answer.setAnswer("1");
        question.setAnswer(answer);

        Hint textHint = new Hint();
        textHint.setContent("This is hint #1.");

        Hint textHint2 = new Hint();
        textHint2.setContent("This is hint #2.");

        Hint textHint3 = new Hint();
        textHint3.setContent("This is hint #3.");

        demoTask1.setHints(List.of(textHint, textHint2, textHint3));
        demoTask1.setQuestion(question);

        taskRepo.save(demoTask1);
    }
}