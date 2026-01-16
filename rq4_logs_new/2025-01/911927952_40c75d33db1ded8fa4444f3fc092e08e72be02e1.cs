namespace ghwebhook.Models;

/// <summary>
/// Code Scanning Default Setup
/// </summary>
public class DefaultSetup
{
    /// <summary>
    /// The desired state of code scanning default setup.
    /// </summary>
    [JsonPropertyName("state")]
    public DefaultSetupStateType State { get; set; }

    /// <summary>
    /// Runner type to be used.
    /// </summary>
    [JsonPropertyName("runner_type")]
    public DefaultSetupRunnerType RunnerType { get; set; } = DefaultSetupRunnerType.standard;

    /// <summary>
    /// Runner label to be used if the runner type is labeled.
    /// </summary>
    [JsonPropertyName("runner_label")]
    public string? RunnerLabel { get; set; }

    /// <summary>
    /// CodeQL query suite to be used.
    /// </summary>
    [JsonPropertyName("query_suite")]
    public DefaultSetupQuerySuiteType QuerySuite { get; set; } = DefaultSetupQuerySuiteType.@default;

    /// <summary>
    /// CodeQL languages to be analyzed. Supported values are: c-cpp, csharp, go, java-kotlin, javascript-typescript, python, ruby, swift
    /// </summary>
    [JsonPropertyName("languages")]
    public List<string> Languages { get; set; } = new();
}