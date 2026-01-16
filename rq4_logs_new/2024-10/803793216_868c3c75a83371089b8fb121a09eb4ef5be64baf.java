package org.springframework.samples.gitvision.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHIssue;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.samples.gitvision.project.model.Project;
import org.springframework.samples.gitvision.task.model.Task;
import org.springframework.samples.gitvision.workspace.model.Workspace;
import org.springframework.web.client.RestTemplate;

public class GithubApi {
    
    static RestTemplate restTemplate;

    static {
        restTemplate = new RestTemplate();
    }

    private static <T> T requestClockify(String url, String githubToken, Class<T> clazz) {
        String url_template =  "https://api.github.com" + url;

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        headers.set("Authorization","Bearer " + githubToken);

        HttpEntity<String> request = new HttpEntity<>(headers);
        ResponseEntity<T> response = restTemplate.exchange(url_template, HttpMethod.GET, request, clazz);

        return response.getBody();
    }

    public static GHCommit[] getCommitsByPage(String repositoryName, Integer page, String githubToken){
        String url = String.format("/repos/%s/commits?page=%d", repositoryName, page);
        GHCommit[] ghCommits = requestClockify(url, githubToken, GHCommit[].class);
        return ghCommits;
    }

    public static GHIssue[] getIssuesByPage(String repositoryName, Integer page, String githubToken){
        String url = String.format("/repos/%s/issues?page=%d", repositoryName, page);
        GHIssue[] ghIssues = requestClockify(url, githubToken, GHIssue[].class);
        return ghIssues;
    }

    public static List<Project> getProjectsByWorkspaceId(String workspaceId, String clockifyToken) {
        String url = String.format("/v1/workspaces/%s/projects", workspaceId);
        Project[] projects = requestClockify(url, clockifyToken, Project[].class);
        return Arrays.asList(projects);
    }
    
}