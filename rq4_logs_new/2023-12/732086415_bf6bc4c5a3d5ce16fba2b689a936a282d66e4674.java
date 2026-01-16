package com.netflix.conductor.contribs.listener.archive;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.core.listener.WorkflowStatusListener;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.model.WorkflowModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchivingWorkflowToS3 implements WorkflowStatusListener {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ArchivingWorkflowToS3.class);
    private final ExecutionDAOFacade executionDAOFacade;

    private final ArchivingWorkflowListenerProperties properties;

    private final AmazonS3 s3Client;

    private final String bucketName;
    private final String bucketRegion;
    private final ObjectMapper objectMapper;

    public ArchivingWorkflowToS3(ExecutionDAOFacade executionDAOFacade, ArchivingWorkflowListenerProperties properties) {
        this.executionDAOFacade = executionDAOFacade;
        this.properties = properties;
        bucketName = properties.getWorkflowS3ArchivalDefaultBucketName();
        bucketRegion = properties.getWorkflowS3ArchivalBucketRegion();
        s3Client = AmazonS3ClientBuilder.standard().withRegion(bucketRegion).build();
        objectMapper = new ObjectMapper();
    }

    @Override
    public void onWorkflowCompleted(WorkflowModel workflow) {
        archiveWorkflow(workflow);
    }

    @Override
    public void onWorkflowTerminated(WorkflowModel workflow) {
        archiveWorkflow(workflow);
    }

    private void archiveWorkflow(final WorkflowModel workflow) {
        // Only archive unsuccessful workflows if enabled
        if (!properties.getWorkflowArchiveUnsuccessfulOnly() || !workflow.getStatus().isSuccessful()) {
            final String fileName = workflow.getWorkflowId() + ".json";
            final String filePathPrefix = workflow.getWorkflowName();
            final String fullFilePath = filePathPrefix + '/' + fileName;

            try {
                // Upload workflow as a json file to s3
                s3Client.putObject(bucketName, fullFilePath, objectMapper.writeValueAsString(workflow));
                LOGGER.info(
                        "Successfully archived workflow {} to S3 bucket {} as file {}",
                        workflow.getWorkflowId(),
                        bucketName,
                        fullFilePath);
            } catch (final TerminateWorkflowException e) {
                throw e;
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
        this.executionDAOFacade.removeWorkflow(workflow.getWorkflowId(), true);
        Monitors.recordWorkflowArchived(workflow.getWorkflowName(), workflow.getStatus());
    }
}