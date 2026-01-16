namespace ghwebhook;

/// <summary>
/// GitHubWebhookEventProcessor that processes all GitHub webhook events.
/// </summary>
/// <param name="gitHubClient">GitHubClient for GitHhub API operation</param>
/// <param name="logger">ILogger</param>
public class GitHubWebhookEventProcessor(GitHubClient gitHubClient, ILogger<GitHubWebhookEventProcessor> logger) : WebhookEventProcessor
{
    private readonly GitHubClient gitHubClient = gitHubClient;
    private readonly ILogger<GitHubWebhookEventProcessor> logger = logger;
    private readonly string gitHubUserName = Environment.GetEnvironmentVariable("GitHubUserName")!;

    /// <summary>
    /// Handle the repository event.
    /// </summary>
    /// <param name="headers">WebhookHeaders</param>
    /// <param name="repositoryEvent">RepositoryEvent</param>
    /// <param name="action">RepositoryAction</param>
    /// <returns></returns>
    protected override async Task ProcessRepositoryWebhookAsync(WebhookHeaders headers, RepositoryEvent repositoryEvent, RepositoryAction action)
    {
        switch (action)
        {
            case RepositoryActionValue.Created:
                logger.LogInformation("Repository create event.");

                if (repositoryEvent.Organization is null || repositoryEvent.Repository is null)
                {
                    logger.LogError("Organization or Repository is null");
                    return;
                }

                string organizationName = repositoryEvent.Organization.Login;
                string repositoryName = repositoryEvent.Repository.Name;
                long repositoryId = repositoryEvent.Repository.Id;

                // Create a ruleset for the repository to protect main (default) branch(es).
                RuleSet ruleSet = new()
                {
                    Owner = organizationName,
                    Repo = repositoryName,
                    Name = "MyRuleSet",
                    Target = TargetType.branch,
                    Enforcement = RuleEnforcement.active,
                    Conditions = new()
                    {
                        RefName = new()
                        {
                            Include = new() { "~DEFAULT_BRANCH" }
                        }
                    },
                    Rules = new()
                    {
                        new DeletionRule(),
                        new PullRequestRule()
                        {
                            Parameters = new()
                            {
                                RequiredApprovingReviewCount=2,
                                RequiredReviewThreadResolution = true,
                                AllowedMergeMethods = new() { "squash", "merge" }
                            }
                        }
                    }
                };

                await gitHubClient.Connection.Post<RuleSet>(
                    new Uri($"repositories/{repositoryId}/rulesets", UriKind.Relative),
                    ruleSet,
                    "application/json",
                    "application/json");

                // Create an issue to notify user.
                NewIssue issue = new("Ruleset has been created")
                {
                    Body = $"Hi @{gitHubUserName}, Ruleset has been created."
                };
                await gitHubClient.Issue.Create(repositoryId, issue);
                break;
            default:
                logger.LogInformation("Some other repository event");
                await Task.Delay(1000);
                break;
        }
    }
}