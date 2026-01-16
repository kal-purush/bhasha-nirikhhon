package de.tudresden.inf.st.mathgrass.api.evaluator.evaluators.sage;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.BuildImageCmd;
import com.github.dockerjava.api.command.BuildImageResultCallback;
import com.github.dockerjava.api.command.ListImagesCmd;
import com.github.dockerjava.api.model.Image;
import de.tudresden.inf.st.mathgrass.api.evaluator.executor.Executor;
import de.tudresden.inf.st.mathgrass.api.evaluator.executor.SourceFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Profile("sage-evaluator")
@Component
public class SageEvaluatorPlugin {
    private static final Logger logger = LogManager.getLogger(SageEvaluatorPlugin.class);
    public static final String SAGE_EVALUATOR_IMAGE_NAME = "sage-evaluator";
    public static final String SAGE_EVALUATOR_TAG = "0.1c";
    public static final String SAGE_IMAGE_COMPLETE_TAG = SAGE_EVALUATOR_IMAGE_NAME + ":" + SAGE_EVALUATOR_TAG;
    public static final String SAGE_EVALUATOR_DOCKERFILE_PATH = "./evaluators/sage-evaluator/Dockerfile";
    public static final String SAGE_EVALUATION_DOCKER_ENTRYPOINT = "sage /sage-evaluation/main.py";
    public static final String SAGE_EVALUATION_GRAPH_AS_JSON_PATH = "/sage-evaluation/graph.json";
    public static final String SAGE_EVALUATION_INSTRUCTOR_EVALUATION_PATH = "/sage-evaluation/instructor_evaluation.py";
    private final DockerClient dockerClient;


    public SageEvaluatorPlugin(DockerClient dockerClient) {
        this.dockerClient = dockerClient;
    }

    @PostConstruct
    private void buildSageImage() {
        ListImagesCmd listImagesCmd = dockerClient.listImagesCmd().withImageNameFilter(SAGE_IMAGE_COMPLETE_TAG);
        listImagesCmd.getFilters().put("reference", List.of(SAGE_IMAGE_COMPLETE_TAG));
        List<Image> filterImages = listImagesCmd.exec();
        if (!filterImages.isEmpty()) {
            logger.info("Sage Evaluator image found ({}), skipping container build", SAGE_IMAGE_COMPLETE_TAG);
            return;
        }
        logger.info("Building sage-evaluator image. This might take some time..");
        BuildImageResultCallback buildImageResultCallback = new BuildImageResultCallback();
        BuildImageCmd buildImageCmd =
                dockerClient.buildImageCmd(new File(SAGE_EVALUATOR_DOCKERFILE_PATH)).withTags(Set.of(SAGE_IMAGE_COMPLETE_TAG));
        buildImageCmd.exec(buildImageResultCallback);
        buildImageResultCallback.awaitImageId();
    }

    /**
     * This methods initializes (but not persists) an
     * {@link de.tudresden.inf.st.mathgrass.api.evaluator.executor.Executor} that contains the default settings for a
     * successful evaluation with the Sage Plugin. In order to work correctly, the executor needs an execution
     * descriptor that serves as the main semantic entry point for the evaluation.
     * The docker image generated in {@link #buildSageImage()} calls
     * {@link SageEvaluatorPlugin#SAGE_EVALUATION_DOCKER_ENTRYPOINT}. This script calls a python function with the
     * signature {@code instructor_evaluation(graph: Graph, user_answer)}.
     * The code for a file containing this very function must be specified in the parameter {@code
     * instructorEvaluationExecutionDescriptor}. The {@code graph} is automatically parsed into a Sage Graph object.
     * The {@code user_answer} is a string containing the user answer. The evaluation logic needs to be implemented in
     * this method. A positive evaluation result returns {@code True}, a negative evaluation result {@code False}
     * Example:
     * <pre>
     * from sage.all import *
     *
     * def instructor_evaluation(graph: Graph, user_answer):
     *    if str(len(graph.edges())) == user_answer:
     *        return True
     *    else:
     *        return False
     * </pre>
     *
     * @param instructorEvaluationExecutionDescriptor Execution description/file contents of a python script that is
     *                                                called by the sage evaluator. It must contain a function with
     *                                                the signature {@code instructor_evaluation(graph: Graph,
     *                                                user_answer)} and return either {@code True}, if the {@code
     *                                                user_answer} was correct, {@code False} otherwise
     * @param additionalSourceFiles                   Additional source files that may contain python code. This code
     *                                               may be references in the {@code
     *                                               instructorEvaluationExecutionDescriptor}
     * @return An Executor that evaluates the student answer with given Python/Sage code
     */
    public Executor initializeSageExecutor(@Nonnull String instructorEvaluationExecutionDescriptor,
                                           List<SourceFile> additionalSourceFiles) {
        Executor executor = new Executor();
        executor.setContainerImage(SageEvaluatorPlugin.SAGE_IMAGE_COMPLETE_TAG);

        SourceFile instructorEvaluationSourceFile = new SourceFile();
        instructorEvaluationSourceFile.setContents(instructorEvaluationExecutionDescriptor);
        instructorEvaluationSourceFile.setPath(SAGE_EVALUATION_INSTRUCTOR_EVALUATION_PATH);

        executor.setCustomEntrypoint(SAGE_EVALUATION_DOCKER_ENTRYPOINT);
        List<SourceFile> sourceFileList = new ArrayList<>(List.of(instructorEvaluationSourceFile));
        sourceFileList.addAll(additionalSourceFiles);

        executor.setSourceFiles(sourceFileList);
        executor.setGraphPath(SAGE_EVALUATION_GRAPH_AS_JSON_PATH);

        return executor;
    }
}