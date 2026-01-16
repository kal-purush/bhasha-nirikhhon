using DockerCliWrapper.Docker.Constants;
using DockerCliWrapper.Docker.Container;
using DockerCliWrapper.Infrastructure;
using FluentAssertions;
using Moq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace DockerCliWrapper.Tests
{
    public class GivenADockerContainersInstance
    {
        [Fact]
        public async Task WhenRequestingWithDefaultOptions_ThenResultsAreReturned()
        {
            var shellExecutor = new Mock<IShellExecutor>();

            var output = new StringBuilder();
            output.AppendLine("CONTAINER ID        IMAGE               COMMAND               CREATED             STATUS              PORTS                             NAMES");
            output.AppendLine("9860eb8aa0b3        service1            \"dotnet Service1.…\"   2 weeks ago         Up 27 hours         8002/tcp, 0.0.0.0:31206->80/tcp   test.service1");
            output.AppendLine("00f692571938        service2            \"dotnet Service2.…\"   2 weeks ago         Up 27 hours         8001/tcp, 0.0.0.0:6113->80/tcp    test.service2");

            shellExecutor
                .Setup(e => e.Execute(Commands.Docker, " container ls"))
                .ReturnsAsync(new ShellExecuteResult(true, output.ToString(), ""));

            var results = await new DockerContainers(shellExecutor.Object).SearchAsync();

            results.Count.Should().Be(2);
            results[0].Id.Should().Be("9860eb8aa0b3");
            results[0].Image.Should().Be("service1");
            results[0].Command.Should().Be("\"dotnet Service1.…\"");
            results[0].Created.Should().Be("2 weeks ago");
            results[0].Status.Should().Be("Up 27 hours");
            results[0].Ports.Should().Be("8002/tcp, 0.0.0.0:31206->80/tcp");
            results[0].Names.Should().Be("test.service1");
            results[0].Size.Should().BeNullOrEmpty();
        }

        [Fact]
        public async Task WhenRequestingSize_ThenResultsAreReturned()
        {
            var shellExecutor = new Mock<IShellExecutor>();

            var output = new StringBuilder();
            output.AppendLine("CONTAINER ID        IMAGE               COMMAND               CREATED             STATUS              PORTS                             NAMES          SIZE");
            output.AppendLine("9860eb8aa0b3        service1            \"dotnet Service1.…\"   2 weeks ago         Up 27 hours         8002/tcp, 0.0.0.0:31206->80/tcp   test.service1  10B");
            output.AppendLine("00f692571938        service2            \"dotnet Service2.…\"   2 weeks ago         Up 27 hours         8001/tcp, 0.0.0.0:6113->80/tcp    test.service2  20B");

            shellExecutor
                .Setup(e => e.Execute(Commands.Docker, " container ls -s"))
                .ReturnsAsync(new ShellExecuteResult(true, output.ToString(), ""));

            var results = await new DockerContainers(shellExecutor.Object).ShowSizes(true).SearchAsync();

            results.Count.Should().Be(2);
            results[0].Id.Should().Be("9860eb8aa0b3");
            results[0].Image.Should().Be("service1");
            results[0].Command.Should().Be("\"dotnet Service1.…\"");
            results[0].Created.Should().Be("2 weeks ago");
            results[0].Status.Should().Be("Up 27 hours");
            results[0].Ports.Should().Be("8002/tcp, 0.0.0.0:31206->80/tcp");
            results[0].Names.Should().Be("test.service1");
            results[0].Size.Should().Be("10B");
        }
    }
}