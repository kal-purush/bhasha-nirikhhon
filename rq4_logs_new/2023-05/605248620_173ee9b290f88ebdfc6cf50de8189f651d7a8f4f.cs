using malimo.TestDirectoryHelper;
using TestProcessWrapper;
using Xunit;
using Xunit.Sdk;

namespace malimo.Acceptance.Tests.Steps;

[Binding]
public sealed class MalimoStepDefinitions
{
    private const string TestDirectoryKey = "TestDirectory";

    private readonly TestOutputHelper _testOutputHelper;

    private readonly ScenarioContext _scenarioContext;

    private string SourceDirectory =>
        ((TestDirectory?)_scenarioContext[TestDirectoryKey])?.SourceDir.FullName ?? "(null)";

    private string TargetDirectory =>
        ((TestDirectory?)_scenarioContext[TestDirectoryKey])?.TargetDir.FullName ?? "(null)";

    private readonly Dictionary<string, string> _arguments = new();

    private bool _hasExitedGracefully;

    private string _recordedOutput = "";

    public MalimoStepDefinitions(TestOutputHelper testOutputHelper, ScenarioContext scenarioContext)
    {
        _testOutputHelper = testOutputHelper;
        _scenarioContext = scenarioContext;
    }

    [BeforeScenario]
    public void BeforeScenario()
    {
        _scenarioContext[TestDirectoryKey] = TestDirectory.Create();
    }

    [AfterScenario]
    public void AfterScenario() => ((TestDirectory?)_scenarioContext[TestDirectoryKey])?.Dispose();

    [Given(@"the option ""(.*)"" is added")]
    public void GivenTheOptionIsAdded(string option) => _arguments[option] = "";

    [Given(@"the file is ""(.*)""")]
    public void GivenTheFileIs(string fileName) => _arguments["--file"] = Path.Combine(SourceDirectory, fileName);

    [Given(@"the target directory is configured")]
    public void GivenTheTargetDirectoryIsConfigured() => _arguments["--target-dir"] = Path.Combine(TargetDirectory);

    [When(@"malimo is executed")]
    public void WhenMalimoIsExecuted()
    {
#if DEBUG
        const BuildConfiguration buildConfiguration = BuildConfiguration.Debug;
#else
        const BuildConfiguration buildConfiguration = BuildConfiguration.Release;
#endif
        using var process = new TestProcessWrapper.TestProcessWrapper("malimo", true, buildConfiguration);
        process.TestOutputHelper = _testOutputHelper;

        foreach (var (argument, value) in _arguments)
        {
            process.AddCommandLineArgument(argument, value);
        }

        process.Start();
        process.WaitForProcessExit();

        _hasExitedGracefully = process.HasExited;
        _recordedOutput = process.RecordedOutput;

        process.ForceTermination();
    }

    [Then(@"the output matches the regex ""(.*)""")]
    public void ThenTheOutputMatchesTheRegex(string regex) => Assert.Matches(regex, _recordedOutput);

    [Then(@"malimo has exited gracefully")]
    public void ThenMalimoHasExitedGracefully() => Assert.True(_hasExitedGracefully, "malimo should exit gracefully");

    [Then(@"the file ""(.*)"" exists in the source directory")]
    public void ThenTheFileExistsInTheSourceDirectory(string fileName)
    {
        var file = new FileInfo(Path.Combine(SourceDirectory, fileName));
        Assert.True(file.Exists, $"file '{file.FullName}' should exist");
    }

    [Then(@"the file ""(.*)"" does not exist in the source directory")]
    public void ThenTheFileDoesNotExistInTheSourceDirectory(string fileName)
    {
        var file = new FileInfo(Path.Combine(SourceDirectory, fileName));
        Assert.False(file.Exists, $"file '{file.FullName}' should not exist");
    }

    [Then(@"the file ""(.*)"" exists in the target directory")]
    public void ThenTheFileExistsInTheTargetDirectory(string fileName)
    {
        var file = new FileInfo(Path.Combine(TargetDirectory, fileName));
        Assert.True(file.Exists, $"file '{file.FullName}' should exist");
    }

    [Then(@"the file ""(.*)"" does not exist in the target directory")]
    public void ThenTheFileDoesNotExistInTheTargetDirectory(string fileName)
    {
        var file = new FileInfo(Path.Combine(TargetDirectory, fileName));
        Assert.False(file.Exists, $"file '{file.FullName}' should not exist");
    }
}