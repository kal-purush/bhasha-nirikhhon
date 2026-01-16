using System.Globalization;

namespace malimo.Integration.Tests;

public sealed class TestDirectory : IDisposable
{
    private DirectoryInfo _baseDir;

    public DirectoryInfo SourceDir { get; private set; }

    public DirectoryInfo TargetDir { get; private set; }

    public void Dispose() => Delete();

    public static TestDirectory Create()
    {
        var result = new TestDirectory();
        result.CreateDirectories();
        result.CopySeedToSource();
        return result;
    }

    private void CreateDirectories()
    {
        var dateString = DateTime.Now.ToString("yyyyMMddTHHmmss", CultureInfo.InvariantCulture);
        var uid = Guid.NewGuid();
        _baseDir = new DirectoryInfo($"data/test-{dateString}-{uid}");
        _baseDir.Create();

        SourceDir = new DirectoryInfo(Path.Combine(_baseDir.FullName, "source"));
        SourceDir.Create();

        TargetDir = new DirectoryInfo(Path.Combine(_baseDir.FullName, "target"));
        TargetDir.Create();
    }

    private void CopySeedToSource()
    {
        CopyDirectoryRecursively(new DirectoryInfo("data/seed"), SourceDir);
    }

    private void CopyDirectoryRecursively(DirectoryInfo sourceDirectory, DirectoryInfo targetDirectory)
    {
        if (!targetDirectory.Exists)
        {
            targetDirectory.Create();
        }

        foreach (var file in sourceDirectory.GetFiles())
        {
            var targetFilePath = Path.Combine(targetDirectory.FullName, file.Name);
            file.CopyTo(targetFilePath);
        }

        foreach (var subdirectory in sourceDirectory.GetDirectories())
        {
            var targetSubdirectory = new DirectoryInfo(Path.Combine(SourceDir.FullName, subdirectory.Name));
            CopyDirectoryRecursively(subdirectory, targetSubdirectory);
        }
    }

    private void Delete() => _baseDir?.Delete(recursive: true);

    private TestDirectory() { }
}