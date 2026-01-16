package edu.java.clients;

import edu.java.dto.github.GitHubDTO;
import org.springframework.web.reactive.function.client.WebClient;

public class GitHubClient {
    private final WebClient webClient;

    public GitHubClient(String baseUrl) {
        String baseUrlDefault = "https://api.github.com";
        if (!baseUrl.isEmpty()) {
            baseUrlDefault = baseUrl;
        }

        webClient = WebClient.builder()
            .baseUrl(baseUrlDefault)
            .build();
    }

    public GitHubDTO fetchRepo(String owner, String repo) {
        return webClient.get()
            .uri("/repos/{owner}/{repo}", owner, repo)
            .retrieve()
            .bodyToMono(GitHubDTO.class)
            .block();
    }
}