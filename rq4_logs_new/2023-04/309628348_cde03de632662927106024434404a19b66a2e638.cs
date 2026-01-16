using Trakx.Common.Testing.Documentation.GenerateApiClient;

namespace Trakx.Shrimpy.ApiClient.Tests.Integration;

public class GenerateApiClientChecker : GenerateApiClientCheckerBase
{
    public GenerateApiClientChecker(ITestOutputHelper output)
        : base(output, CreateProjectFileFinder())
    {
    }

    private static IProjectFileFinder CreateProjectFileFinder()
    {
        var objectFromClientAssembly = new ShrimpyApiConfiguration();
        var currentDirectory = Environment.CurrentDirectory;
        return new ProjectFileFinder(objectFromClientAssembly, currentDirectory);
    }
}