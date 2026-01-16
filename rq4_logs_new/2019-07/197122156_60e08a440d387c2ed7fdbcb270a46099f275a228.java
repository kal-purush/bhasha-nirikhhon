package com.mannanlive;

import com.amazonaws.services.codecommit.model.Commit;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.CodeCommitEvent;
import com.mannanlive.service.WorkflowTool;
import com.mannanlive.service.CodeCommitRepository;
import com.mannanlive.service.WorkflowToolFactory;

public class CodeCommitHandler implements RequestHandler<CodeCommitEvent, Void> {

    private final CodeCommitRepository codeCommit = new CodeCommitRepository();

    public Void handleRequest(final CodeCommitEvent input, final Context context) {
        input.getRecords().forEach(record -> {
            final WorkflowTool workflowTool = new WorkflowToolFactory().create(record);
            System.out.println(String.format("region=%s, service=%s",
                    record.getAwsRegion(), workflowTool.getRepositoryName()));

            record.getCodeCommit().getReferences().forEach(reference -> {
                final Commit commit = codeCommit.getCommit(workflowTool.getRepositoryName(), reference.getCommit());
                System.out.println(String.format("commit=%s, message=%s", reference.getCommit(), commit.getMessage()));
                workflowTool.process(commit);
            });
        });
        return null;
    }
}