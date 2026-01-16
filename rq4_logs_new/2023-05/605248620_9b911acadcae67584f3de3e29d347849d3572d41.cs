using malimo.TestDirectoryHelper;
using TestProcessWrapper;
using Xunit;
using Xunit.Sdk;

namespace malimo.Acceptance.Tests.Steps;

[Binding]
public sealed class VerboseDryRunStepDefinitions : IDisposable
{
    private readonly TestOutputHelper _testOutputHelper;
    private static TestDirectory? _testDirectory;
    private readonly Dictionary<string, string> _arguments = new();
    private TestProcessWrapper.TestProcessWrapper? _process;
    private bool _hasExitedGracefully;

    public VerboseDryRunStepDefinitions(TestOutputHelper testOutputHelper) => _testOutputHelper = testOutputHelper;

    [BeforeScenario]
    public static void SetupTestDirectory() => _testDirectory = TestDirectory.Create();

    [Given(@"the option ""(.*)"" is added")]
    public void GivenTheOptionIsAdded(string option) => _arguments[option] = "";

    [Given(@"the file is ""(.*)""")]
    public void GivenTheFileIs(string fileName)
    {
        var sourceDirectory = _testDirectory?.SourceDir.FullName ?? "(null)";
        _arguments["--file"] = Path.Combine(sourceDirectory, fileName);
    }

    [Given(@"the target directory is configured")]
    public void GivenTheTargetDirectoryIsConfigured()
    {
        var targetDirectory = _testDirectory?.TargetDir.FullName ?? "(null)";
        _arguments["--target-dir"] = Path.Combine(targetDirectory);
    }

    [When(@"malimo is executed")]
    public void WhenMalimoIsExecuted()
    {
#if DEBUG
        const BuildConfiguration buildConfiguration = BuildConfiguration.Debug;
#else
        const BuildConfiguration buildConfiguration = BuildConfiguration.Release;
#endif
        _process = new TestProcessWrapper.TestProcessWrapper("malimo", false, buildConfiguration);
        _process.TestOutputHelper = _testOutputHelper;

        foreach (var (argument, value) in _arguments)
        {
            _process.AddCommandLineArgument(argument, value);
        }

        _process.Start();
        _process.WaitForProcessExit();

        _hasExitedGracefully = _process.HasExited;

        _process.ForceTermination();
    }

    [Then(@"the output matches the regex ""(.*)""")]
    public void ThenTheOutputMatchesTheRegex(string regex) =>
        Assert.Matches(regex, _process?.RecordedOutput ?? "(null)");

    public void Dispose() => _testDirectory?.Dispose();

    [Then(@"malimo has exited gracefully")]
    public void ThenMalimoHasExitedGracefully() => Assert.True(_hasExitedGracefully, "malimo should exit gracefully");
}