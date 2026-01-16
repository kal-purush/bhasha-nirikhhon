

using Microsoft.AspNetCore.Mvc.Infrastructure;
using Microsoft.Azure.Functions.Worker.Core.FunctionMetadata;
using Microsoft.Extensions.Logging;

namespace ghwebhook.UnitTests;

/// <summary>
/// Unit Tests for GitHubWebhookEventProcessor.cs
/// </summary>
public class GitHubWebhookEventProcessorUnitTests
{
    [Fact]
    public async Task HandleRepositoryCreationEventAsync_Organization_Is_Null_Return()
    {
        // Arrange
        Mock<IGitHubClient> mockIGitHubClient = new();
        Mock<ILogger<GitHubWebhookEventProcessor>> mockILogger = new();
        GitHubWebhookEventProcessor processor = new GitHubWebhookEventProcessor(mockIGitHubClient.Object, mockILogger.Object);
        WebhookHeaders headers = new();
        Octokit.Webhooks.Models.Repository repository = new() { Name = "test-repo", Id = 123 };

        // Act
        await processor.HandleRepositoryCreationEventAsync(null, repository);

        // Assert
        mockILogger.Verify(x => x.Log<It.IsAnyType>(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => string.Equals("Organization or Repository is null", o.ToString())),
            It.IsAny<Exception>(), // Whatever exception may have been logged with it, change as needed.
            (Func<It.IsAnyType, Exception?, string>)It.IsAny<object>()), Times.Once);
    }

    [Fact]
    public async Task HandleRepositoryCreationEventAsync_Repository_Is_Null_Return()
    {
        // Arrange
        string organizationName = "org";
        Mock<IGitHubClient> mockIGitHubClient = new();
        Mock<ILogger<GitHubWebhookEventProcessor>> mockILogger = new();
        GitHubWebhookEventProcessor processor = new GitHubWebhookEventProcessor(mockIGitHubClient.Object, mockILogger.Object);
        WebhookHeaders headers = new();
        Octokit.Webhooks.Models.Organization organization = new() { Login = organizationName };

        // Act
        await processor.HandleRepositoryCreationEventAsync(organization, null);

        // Assert
        mockILogger.Verify(x => x.Log<It.IsAnyType>(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => string.Equals("Organization or Repository is null", o.ToString())),
            It.IsAny<Exception>(), // Whatever exception may have been logged with it, change as needed.
            (Func<It.IsAnyType, Exception?, string>)It.IsAny<object>()), Times.Once);
    }

    [Fact]
    public async Task HandleRepositoryCreationEventAsync_All_Created_Issue_With_Message()
    {
        // Arrange
        Environment.SetEnvironmentVariable("GitHubUserName", "test-user");
        long repositoryId = 123;
        string repositoryName = "test-repo";
        string organizationName = "org";
        Mock<IConnection> mockIConnection = new();
        mockIConnection.Setup(x => x.Post<RuleSet>(
            It.IsAny<Uri>(),
            It.IsAny<RuleSet>(),
            "application/json",
            "application/json",
            It.IsAny<IDictionary<string, string>>(),
            CancellationToken.None)).ReturnsAsync(new Mock<IApiResponse<RuleSet>>().Object);
        mockIConnection.Setup(x => x.Patch<DefaultSetup>(It.IsAny<Uri>(),
            It.IsAny<DefaultSetup>(),
            "application/json")).ReturnsAsync(new Mock<IApiResponse<DefaultSetup>>().Object);
        Mock<IRepositoriesClient> mockIRepositoriesClient = new();
        mockIRepositoriesClient.Setup(x => x.Edit(repositoryId, It.IsAny<RepositoryUpdate>())).ReturnsAsync(new Repository());
        Mock<IIssuesClient> mockIIssuesClient = new();
        mockIIssuesClient.Setup(x => x.Create(repositoryId, It.IsAny<NewIssue>())).ReturnsAsync(new Issue());
        Mock<IGitHubClient> mockIGitHubClient = new();
        mockIGitHubClient.Setup(x => x.Connection).Returns(mockIConnection.Object);
        mockIGitHubClient.Setup(x => x.Repository).Returns(mockIRepositoriesClient.Object);
        mockIGitHubClient.Setup(x => x.Issue).Returns(mockIIssuesClient.Object);
        Mock<ILogger<GitHubWebhookEventProcessor>> mockILogger = new();
        GitHubWebhookEventProcessor processor = new GitHubWebhookEventProcessor(mockIGitHubClient.Object, mockILogger.Object);
        WebhookHeaders headers = new();
        Octokit.Webhooks.Models.Organization organization = new() { Login = organizationName };
        Octokit.Webhooks.Models.Repository repository = new() { Name = repositoryName, Id = repositoryId };

        // Act
        await processor.HandleRepositoryCreationEventAsync(organization, repository);

        // Assert
        mockILogger.Verify(x => x.Log<It.IsAnyType>(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.IsAny<It.IsAnyType>(),
            It.IsAny<Exception>(), // Whatever exception may have been logged with it, change as needed.
            (Func<It.IsAnyType, Exception?, string>)It.IsAny<object>()), Times.Never);
        mockIConnection.Verify(x => x.Post<RuleSet>(
            It.Is<Uri>(x => x.ToString() == $"repositories/{repositoryId}/rulesets"),
            It.Is<RuleSet>(r =>
                r.Repo == repositoryName &&
                r.Owner == organizationName &&
                r.Name == "MyRuleSet" &&
                r.Target == TargetType.branch &&
                r.Enforcement == RuleEnforcement.active &&
                r.Conditions.RefName.Include.First() == "~DEFAULT_BRANCH" &&
                r.Rules[0] is DeletionRule &&
                r.Rules[1] is PullRequestRule &&
                (r.Rules[1] as PullRequestRule)!.Parameters.RequiredApprovingReviewCount == 2 &&
                (r.Rules[1] as PullRequestRule)!.Parameters.RequiredReviewThreadResolution == true &&
                (r.Rules[1] as PullRequestRule)!.Parameters.AllowedMergeMethods.Contains("squash") &&
                (r.Rules[1] as PullRequestRule)!.Parameters.AllowedMergeMethods.Contains("merge")),
            "application/json",
            "application/json",
            It.IsAny<IDictionary<string, string>>(),
            CancellationToken.None),
            Times.Once);
        mockIConnection.Verify(x => x.Patch<DefaultSetup>(
            It.Is<Uri>(x => x.ToString() == $"repositories/{repositoryId}/code-scanning/default-setup"),
            It.Is<DefaultSetup>(d => d.State == DefaultSetupStateType.Configured),
            "application/json"),
            Times.Once);
        mockIRepositoriesClient.Verify(x => x.Edit(
            repositoryId,
            It.Is<RepositoryUpdate>(r =>
                r.SecurityAndAnalysis.SecretScanning.Status == Status.Enabled &&
                r.SecurityAndAnalysis.SecretScanningPushProtection.Status == Status.Enabled)),
            Times.Once);
        mockIIssuesClient.Verify(x => x.Create(repositoryId, It.Is<NewIssue>(i => i.Body == """
        Hi @test-user,
        Ruleset has been created successfully.
        Default setup has been configured successfully.
        Secret scanning has been enabled successfully.

        """)), Times.Once);
    }

    [Fact]
    public async Task HandleRepositoryCreationEventAsync_RuleSet_Fails_Issue_With_Error()
    {
        // Arrange
        Environment.SetEnvironmentVariable("GitHubUserName", "test-user");
        string organizationName = "org";
        long repositoryId = 123;
        Mock<IConnection> mockIConnection = new();
        mockIConnection.Setup(x=>x.Post<RuleSet>(
            It.IsAny<Uri>(),
            It.IsAny<RuleSet>(),
            "application/json",
            "application/json",
            It.IsAny<IDictionary<string,string>>(),
            CancellationToken.None)).ThrowsAsync(new Exception("Error"));
        mockIConnection.Setup(x => x.Patch<DefaultSetup>(It.IsAny<Uri>(),
            It.IsAny<DefaultSetup>(),
            "application/json")).ReturnsAsync(new Mock<IApiResponse<DefaultSetup>>().Object);
        Mock<IRepositoriesClient> mockIRepositoriesClient = new();
        mockIRepositoriesClient.Setup(x => x.Edit(repositoryId, It.IsAny<RepositoryUpdate>())).ReturnsAsync(new Repository());
        Mock<IIssuesClient> mockIIssuesClient = new();
        mockIIssuesClient.Setup(x=>x.Create(repositoryId, It.IsAny<NewIssue>())).ReturnsAsync(new Issue());
        Mock<IGitHubClient> mockIGitHubClient = new();
        mockIGitHubClient.Setup(x => x.Connection).Returns(mockIConnection.Object);
        mockIGitHubClient.Setup(x => x.Repository).Returns(mockIRepositoriesClient.Object);
        mockIGitHubClient.Setup(x => x.Issue).Returns(mockIIssuesClient.Object);
        Mock<ILogger<GitHubWebhookEventProcessor>> mockILogger = new();
        GitHubWebhookEventProcessor processor = new GitHubWebhookEventProcessor(mockIGitHubClient.Object, mockILogger.Object);
        WebhookHeaders headers = new();
        Octokit.Webhooks.Models.Organization organization = new() { Login = organizationName };
        Octokit.Webhooks.Models.Repository repository = new() { Name = "test-repo", Id = repositoryId };

        // Act
        await processor.HandleRepositoryCreationEventAsync(organization, repository);

        // Assert
        mockILogger.Verify(x => x.Log<It.IsAnyType>(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => string.Equals("Failed to create ruleset.", o.ToString())),
            It.IsAny<Exception>(), // Whatever exception may have been logged with it, change as needed.
            (Func<It.IsAnyType, Exception?, string>)It.IsAny<object>()), Times.Once);
        mockIIssuesClient.Verify(x => x.Create(repositoryId, It.Is<NewIssue>(i => i.Body == """
        Hi @test-user,
        Failed to create ruleset.
        Default setup has been configured successfully.
        Secret scanning has been enabled successfully.

        """)), Times.Once);
    }

    [Fact]
    public async Task HandleRepositoryCreationEventAsync_CodeScanning_Fails_Issue_With_Error()
    {
        // Arrange
        Environment.SetEnvironmentVariable("GitHubUserName", "test-user");
        string organizationName = "org";
        long repositoryId = 123;
        Mock<IConnection> mockIConnection = new();
        mockIConnection.Setup(x => x.Post<RuleSet>(
            It.IsAny<Uri>(),
            It.IsAny<RuleSet>(),
            "application/json",
            "application/json",
            It.IsAny<IDictionary<string, string>>(),
            CancellationToken.None)).ReturnsAsync(new Mock<IApiResponse<RuleSet>>().Object);
        mockIConnection.Setup(x => x.Patch<DefaultSetup>(It.IsAny<Uri>(),
            It.IsAny<DefaultSetup>(),
            "application/json")).ThrowsAsync(new Exception("Error")); 
        Mock<IRepositoriesClient> mockIRepositoriesClient = new();
        mockIRepositoriesClient.Setup(x => x.Edit(repositoryId, It.IsAny<RepositoryUpdate>())).ReturnsAsync(new Repository());
        Mock<IIssuesClient> mockIIssuesClient = new();
        mockIIssuesClient.Setup(x => x.Create(repositoryId, It.IsAny<NewIssue>())).ReturnsAsync(new Issue());
        Mock<IGitHubClient> mockIGitHubClient = new();
        mockIGitHubClient.Setup(x => x.Connection).Returns(mockIConnection.Object);
        mockIGitHubClient.Setup(x => x.Repository).Returns(mockIRepositoriesClient.Object);
        mockIGitHubClient.Setup(x => x.Issue).Returns(mockIIssuesClient.Object);
        Mock<ILogger<GitHubWebhookEventProcessor>> mockILogger = new();
        GitHubWebhookEventProcessor processor = new GitHubWebhookEventProcessor(mockIGitHubClient.Object, mockILogger.Object);
        WebhookHeaders headers = new();
        Octokit.Webhooks.Models.Organization organization = new() { Login = organizationName };
        Octokit.Webhooks.Models.Repository repository = new() { Name = "test-repo", Id = repositoryId };

        // Act
        await processor.HandleRepositoryCreationEventAsync(organization, repository);

        // Assert
        mockILogger.Verify(x => x.Log<It.IsAnyType>(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => string.Equals("Failed to configure default setup.", o.ToString())),
            It.IsAny<Exception>(), // Whatever exception may have been logged with it, change as needed.
            (Func<It.IsAnyType, Exception?, string>)It.IsAny<object>()), Times.Once);
        mockIIssuesClient.Verify(x => x.Create(repositoryId, It.Is<NewIssue>(i => i.Body == """
        Hi @test-user,
        Ruleset has been created successfully.
        Failed to configure default setup.
        Secret scanning has been enabled successfully.

        """)), Times.Once);
    }

    [Fact]
    public async Task HandleRepositoryCreationEventAsync_SecurityScan_Fails_Issue_With_Error()
    {
        // Arrange
        Environment.SetEnvironmentVariable("GitHubUserName", "test-user");
        string organizationName = "org";
        long repositoryId = 123;
        Mock<IConnection> mockIConnection = new();
        mockIConnection.Setup(x => x.Post<RuleSet>(
            It.IsAny<Uri>(),
            It.IsAny<RuleSet>(),
            "application/json",
            "application/json",
            It.IsAny<IDictionary<string, string>>(),
            CancellationToken.None)).ReturnsAsync(new Mock<IApiResponse<RuleSet>>().Object);
        mockIConnection.Setup(x => x.Patch<DefaultSetup>(It.IsAny<Uri>(),
            It.IsAny<DefaultSetup>(),
            "application/json")).ReturnsAsync(new Mock<IApiResponse<DefaultSetup>>().Object);
        Mock<IRepositoriesClient> mockIRepositoriesClient = new();
        mockIRepositoriesClient.Setup(x => x.Edit(repositoryId, It.IsAny<RepositoryUpdate>())).ThrowsAsync(new Exception());
        Mock<IIssuesClient> mockIIssuesClient = new();
        mockIIssuesClient.Setup(x => x.Create(repositoryId, It.IsAny<NewIssue>())).ReturnsAsync(new Issue());
        Mock<IGitHubClient> mockIGitHubClient = new();
        mockIGitHubClient.Setup(x => x.Connection).Returns(mockIConnection.Object);
        mockIGitHubClient.Setup(x => x.Repository).Returns(mockIRepositoriesClient.Object);
        mockIGitHubClient.Setup(x => x.Issue).Returns(mockIIssuesClient.Object);
        Mock<ILogger<GitHubWebhookEventProcessor>> mockILogger = new();
        GitHubWebhookEventProcessor processor = new GitHubWebhookEventProcessor(mockIGitHubClient.Object, mockILogger.Object);
        WebhookHeaders headers = new();
        Octokit.Webhooks.Models.Organization organization = new() { Login = organizationName };
        Octokit.Webhooks.Models.Repository repository = new() { Name = "test-repo", Id = repositoryId };

        // Act
        await processor.HandleRepositoryCreationEventAsync(organization, repository);

        // Assert
        mockILogger.Verify(x => x.Log<It.IsAnyType>(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => string.Equals("Failed to enable secret scanning.", o.ToString())),
            It.IsAny<Exception>(), // Whatever exception may have been logged with it, change as needed.
            (Func<It.IsAnyType, Exception?, string>)It.IsAny<object>()), Times.Once);
        mockIIssuesClient.Verify(x => x.Create(repositoryId, It.Is<NewIssue>(i => i.Body == """
        Hi @test-user,
        Ruleset has been created successfully.
        Default setup has been configured successfully.
        Failed to enable secret scanning.

        """)), Times.Once);
    }

    [Fact]
    public async Task HandleRepositoryCreationEventAsync_Issue_Fails_Log_Error()
    {
        // Arrange
        Environment.SetEnvironmentVariable("GitHubUserName", "test-user");
        string organizationName = "org";
        long repositoryId = 123;
        Mock<IConnection> mockIConnection = new();
        mockIConnection.Setup(x => x.Post<RuleSet>(
            It.IsAny<Uri>(),
            It.IsAny<RuleSet>(),
            "application/json",
            "application/json",
            It.IsAny<IDictionary<string, string>>(),
            CancellationToken.None)).ReturnsAsync(new Mock<IApiResponse<RuleSet>>().Object);
        mockIConnection.Setup(x => x.Patch<DefaultSetup>(It.IsAny<Uri>(),
            It.IsAny<DefaultSetup>(),
            "application/json")).ReturnsAsync(new Mock<IApiResponse<DefaultSetup>>().Object);
        Mock<IRepositoriesClient> mockIRepositoriesClient = new();
        mockIRepositoriesClient.Setup(x => x.Edit(repositoryId, It.IsAny<RepositoryUpdate>())).ReturnsAsync(new Repository());
        Mock<IIssuesClient> mockIIssuesClient = new();
        mockIIssuesClient.Setup(x => x.Create(repositoryId, It.IsAny<NewIssue>())).ThrowsAsync(new Exception());
        Mock<IGitHubClient> mockIGitHubClient = new();
        mockIGitHubClient.Setup(x => x.Connection).Returns(mockIConnection.Object);
        mockIGitHubClient.Setup(x => x.Repository).Returns(mockIRepositoriesClient.Object);
        mockIGitHubClient.Setup(x => x.Issue).Returns(mockIIssuesClient.Object);
        Mock<ILogger<GitHubWebhookEventProcessor>> mockILogger = new();
        GitHubWebhookEventProcessor processor = new GitHubWebhookEventProcessor(mockIGitHubClient.Object, mockILogger.Object);
        WebhookHeaders headers = new();
        Octokit.Webhooks.Models.Organization organization = new() { Login = organizationName };
        Octokit.Webhooks.Models.Repository repository = new() { Name = "test-repo", Id = repositoryId };

        // Act
        await processor.HandleRepositoryCreationEventAsync(organization, repository);

        // Assert
        mockILogger.Verify(x => x.Log<It.IsAnyType>(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => string.Equals("Failed to create issue.", o.ToString())),
            It.IsAny<Exception>(), // Whatever exception may have been logged with it, change as needed.
            (Func<It.IsAnyType, Exception?, string>)It.IsAny<object>()), Times.Once);
        mockIIssuesClient.Verify(x => x.Create(repositoryId, It.Is<NewIssue>(i => i.Body == """
        Hi @test-user,
        Ruleset has been created successfully.
        Default setup has been configured successfully.
        Secret scanning has been enabled successfully.

        """)), Times.Once);
    }
}