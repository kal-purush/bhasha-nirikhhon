package org.springframework.samples.gitvision.issue;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.samples.gitvision.auth.payload.response.MessageResponse;
import org.springframework.samples.gitvision.issue.model.Issue;
import org.springframework.samples.gitvision.util.Information;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;


@RestController
@RequestMapping("/api/v1/issues")
public class IssueController {
    
    @Autowired
    IssueService issueService;

    @GetMapping("/{owner}/{repo}")
    public MessageResponse getAllIssuesByRepositoryName(@PathVariable String owner, 
                                                    @PathVariable String repo, 
                                                    @RequestParam String login) {
        String repositoryName = owner + "/" + repo;
        List<Issue> issues = this.issueService.getAllIssuesByRepositoryName(repositoryName, login);
        Information information = Information.create("issues", issues);
        return MessageResponse.of(information);
    }

    @GetMapping("/{owner}/{repo}/{issueNumber}")
    public MessageResponse getIssueByRepositoryNameAndIssueNumber(@PathVariable String owner, 
                                                    @PathVariable String repo, 
                                                    @PathVariable Integer issueNumber, 
                                                    @RequestParam String login) {
        String repositoryName = owner + "/" + repo;
        Issue issue = this.issueService.getIssueByRepositoryNameAndIssueNumber(repositoryName, issueNumber, login);
        Information information = Information.create("issue", issue);
        return MessageResponse.of(information);
    }

}