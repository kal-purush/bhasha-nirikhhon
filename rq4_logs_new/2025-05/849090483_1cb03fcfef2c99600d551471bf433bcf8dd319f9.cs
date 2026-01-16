using L5Sharp.Core;
using LogixShell;
using Shouldly;

namespace LogixCli.Tests.cmd;

[TestFixture]
public class ImportLogixProjectCommandTests
{
    [Test]
    public void Invoke_KnownTestProject_ShouldNotHaveEmptyOutput()
    {
        var command = new ImportLogixProjectCommand
        {
            FilePath = [Known.TestFile]
        };

        var result = command.Invoke<L5X>().ToList();

        result.ShouldHaveSingleItem();
    }

    [Test]
    public void Invoke_WithInvalidPath_ShouldThrowException()
    {
        var command = new ImportLogixProjectCommand { FilePath = [Known.FakeFile] };

        Should.Throw<FileNotFoundException>(() => command.Invoke().Cast<Exception>().ToList());
    }

    [Test]
    public void Invoke_WithMultipleFiles_ShouldProcessAll()
    {
        var command = new ImportLogixProjectCommand
        {
            FilePath = [Known.TestFile, Known.TestFile]
        };

        var result = command.Invoke<L5X>().ToList();

        result.Count.ShouldBe(2);
    }

    [Test]
    public void Invoke_WithIndexEnabled_ShouldProcessSuccessfully()
    {
        var command = new ImportLogixProjectCommand
        {
            FilePath = [Known.TestFile],
            Index = true
        };

        var result = command.Invoke<L5X>().ToList();

        result.ShouldHaveSingleItem();
    }

    [Test]
    public void Invoke_WithInvalidContent_ShouldThrowException()
    {
        var command = new ImportLogixProjectCommand { FilePath = [Known.FakeFile] };

        Should.Throw<Exception>(() => command.Invoke().Cast<Exception>().ToList());
    }
}