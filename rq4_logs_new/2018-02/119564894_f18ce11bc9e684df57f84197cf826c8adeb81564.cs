using DockerCliWrapper.Docker.Constants;
using DockerCliWrapper.Docker.Status;
using DockerCliWrapper.Infrastructure;
using FluentAssertions;
using Moq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace DockerCliWrapper.Tests
{
    public class GivenADockerStatusInstance
    {
        [Fact]
        public async Task WhenConnectingAndServerIsDown_ThenServerStatusIsNotRunning()
        {
            var output = new StringBuilder();
            output.AppendLine("Client: ");
            output.AppendLine(" Version:       17.12.0-ce");
            output.AppendLine(" API version:   1.35");
            output.AppendLine(" Go version:    go1.9.2");
            output.AppendLine(" Git commit:    c97c6d6");
            output.AppendLine(" Built: Wed Dec 27 20:05:22 2017");
            output.AppendLine(" OS/Arch:       windows/amd64");

            var shellExecutor = new Mock<IShellExecutor>();

            shellExecutor
                .Setup(e => e.Execute(Commands.Docker, "version"))
                .ReturnsAsync(new ShellExecuteResult(true, output.ToString(), "error during connect: Get http://%2F%2F.%2Fpipe%2Fdocker_engine/v1.35/version: open //./pipe/docker_engine: The system cannot find the file specified. In the default daemon configuration on Windows, the docker client must be run elevated to connect. This error may also indicate that the docker daemon is not running."));

            var status = new DockerStatus(shellExecutor.Object);

            await status.Connect();

            status.ClientDetails.IsRunning.Should().BeTrue();
            status.ClientDetails.Error.Should().BeNullOrEmpty();
            status.ClientDetails.Version.Should().Be("17.12.0-ce");
            status.ClientDetails.ApiVersion.Should().Be("1.35");
            status.ClientDetails.GoVersion.Should().Be("go1.9.2");
            status.ClientDetails.GitCommit.Should().Be("c97c6d6");
            status.ClientDetails.Built.Should().Be("Wed Dec 27 20:05:22 2017");
            status.ClientDetails.OsArch.Should().Be("windows/amd64");
            status.ClientDetails.Experimental.Should().Be("");

            status.ServerDetails.IsRunning.Should().BeFalse();
            status.ServerDetails.Error.Should().Contain("error during connect: Get http://%2F%2F.%2Fpipe%2Fdocker_engine/v1.35/version: open //./pipe/docker_engine: The system cannot find the file specified. In the default daemon configuration on Windows, the docker client must be run elevated to connect. This error may also indicate that the docker daemon is not running.");
            status.ServerDetails.Version.Should().BeNullOrEmpty();
            status.ServerDetails.ApiVersion.Should().BeNullOrEmpty();
            status.ServerDetails.GoVersion.Should().BeNullOrEmpty();
            status.ServerDetails.GitCommit.Should().BeNullOrEmpty();
            status.ServerDetails.Built.Should().BeNullOrEmpty();
            status.ServerDetails.OsArch.Should().BeNullOrEmpty();
            status.ServerDetails.Experimental.Should().BeNullOrEmpty();
        }

        [Fact]
        public async Task WhenConnectingAndServerIsUp_ThenServerStatusIsPopulated()
        {
            var output = new StringBuilder();
            output.AppendLine("Client: ");
            output.AppendLine(" Version:       17.12.0-ce");
            output.AppendLine(" API version:   1.35");
            output.AppendLine(" Go version:    go1.9.2");
            output.AppendLine(" Git commit:    c97c6d6");
            output.AppendLine(" Built: Wed Dec 27 20:05:22 2017");
            output.AppendLine(" OS/Arch:       windows/amd64");
            output.AppendLine("");
            output.AppendLine("Server: ");
            output.AppendLine(" Engine:");
            output.AppendLine("  Version:       17.12.0-ce");
            output.AppendLine("  API version:   1.35 (minimum version 1.24)");
            output.AppendLine("  Go version:    go1.9.2");
            output.AppendLine("  Git commit:    c97c6d6");
            output.AppendLine("  Built: Wed Dec 27 20:15:52 2017");
            output.AppendLine("  OS/Arch:       windows/amd64");
            output.AppendLine("  Experimental: true");

            var shellExecutor = new Mock<IShellExecutor>();

            shellExecutor
                .Setup(e => e.Execute(Commands.Docker, "version"))
                .ReturnsAsync(new ShellExecuteResult(true, output.ToString(), ""));

            var status = new DockerStatus(shellExecutor.Object);

            await status.Connect();

            status.ClientDetails.IsRunning.Should().BeTrue();
            status.ClientDetails.Error.Should().BeNullOrEmpty();
            status.ClientDetails.Version.Should().Be("17.12.0-ce");
            status.ClientDetails.ApiVersion.Should().Be("1.35");
            status.ClientDetails.GoVersion.Should().Be("go1.9.2");
            status.ClientDetails.GitCommit.Should().Be("c97c6d6");
            status.ClientDetails.Built.Should().Be("Wed Dec 27 20:05:22 2017");
            status.ClientDetails.OsArch.Should().Be("windows/amd64");
            status.ClientDetails.Experimental.Should().Be("");

            status.ServerDetails.IsRunning.Should().BeTrue();
            status.ServerDetails.Error.Should().BeNullOrEmpty();
            status.ServerDetails.Version.Should().Be("17.12.0-ce");
            status.ServerDetails.ApiVersion.Should().Be("1.35 (minimum version 1.24)");
            status.ServerDetails.GoVersion.Should().Be("go1.9.2");
            status.ServerDetails.GitCommit.Should().Be("c97c6d6");
            status.ServerDetails.Built.Should().Be("Wed Dec 27 20:15:52 2017");
            status.ServerDetails.OsArch.Should().Be("windows/amd64");
            status.ServerDetails.Experimental.Should().Be("true");
        }
    }
}