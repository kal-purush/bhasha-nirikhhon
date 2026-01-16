using DockerCliWrapper.Docker.Image;
using DockerCliWrapper.Infrastructure;
using FluentAssertions;
using Moq;
using System;
using System.Text;
using Xunit;

namespace DockerCliWrapper.Tests
{
    public class GivenADockerImageHistoryInstance
    {
        [Fact]
        public void WhenRequestingToBeQuiet_ThenOnlyImageIdsAreReturned()
        {
            var shellExecutor = new Mock<IShellExecutor>();

            shellExecutor
                .Setup(e => e.Execute("docker", " image history -H=True -q imageName"))
                .Returns(new ShellExecuteResult(true, "ecea3d792cd1" + Environment.NewLine + "<missing>", ""));

            var dockerImage = new DockerImage("imageName", shellExecutor.Object);

            var results = dockerImage.History().BeQuiet().Execute();

            results.Count.Should().Be(2);
            results[0].ImageId.Should().Be("ecea3d792cd1");
            results[0].Comment.Should().BeNullOrEmpty();
            results[0].CreatedAt.Should().Be(DateTime.MinValue);
            results[0].CreatedSince.Should().BeNullOrEmpty();
            results[0].Size.Should().BeNullOrEmpty();
            results[1].ImageId.Should().Be("<missing>");
            results[0].Size.Should().BeNullOrEmpty();
        }

        [Fact]
        public void WhenRequestingToNotTruncate_ThenUntruncatedDataIsReturned()
        {
            var output = new StringBuilder();
            output.AppendLine("IMAGE                                                                     CREATED             CREATED BY                                                                                           SIZE                COMMENT");
            output.AppendLine("sha256:ecea3d792cd143caccdf16935cfa1e4d0ec566a6d9e64eac25dfe9087c806702   3 weeks ago         cmd /S /C #(nop)  CMD [\"cmd\" \" / C\" \"type C:\\hello.txt\"]                                              41kB                Comment1");
            output.AppendLine("<missing>                                                                 3 weeks ago         cmd /S /C #(nop) COPY file:f7c8910f60a7ec8d3a775a4b5ae8797e5a3efb9d531b782e2a57d2f65314d2dd in C:    41.8kB                ");
            output.AppendLine("<missing>                                                                 4 weeks ago         Install update 10.0.16299.192                                                                        100MB                ");
            output.AppendLine("<missing>                                                                 4 months ago        Apply image 10.0.16299.15                                                                            203MB                ");

            var shellExecutor = new Mock<IShellExecutor>();

            shellExecutor
                .Setup(e => e.Execute("docker", " image history -H=True --no-trunc imageName"))
                .Returns(new ShellExecuteResult(true, output.ToString(), ""));

            var dockerImage = new DockerImage("imageName", shellExecutor.Object);

            var results = dockerImage.History().DoNotTruncate().Execute();

            results.Count.Should().Be(4);
            results[0].ImageId.Should().Be("sha256:ecea3d792cd143caccdf16935cfa1e4d0ec566a6d9e64eac25dfe9087c806702");
            results[0].CreatedBy.Should().Be("cmd /S /C #(nop)  CMD [\"cmd\" \" / C\" \"type C:\\hello.txt\"]");
            results[0].Comment.Should().Be("Comment1");
            results[0].CreatedAt.Should().Be(DateTime.MinValue);
            results[0].CreatedSince.Should().Be("3 weeks ago");
            results[0].Size.Should().Be("41kB");
        }

        [Fact]
        public void WhenRequestingNonHumanReadableFormat_ThenNonHumanReadableDataIsReturned()
        {
            var output = new StringBuilder();
            output.AppendLine("IMAGE               CREATED AT                  CREATED BY                                      SIZE                COMMENT");
            output.AppendLine("ecea3d792cd1        2018-01-08T20:07:42Z        cmd /S /C #(nop)  CMD [\"cmd\" \" / C\" \"type C:\\…   40960                Comment1");
            output.AppendLine("<missing>           2018-01-08T20:07:42Z        cmd /S /C #(nop) COPY file:f7c8910f60a7ec8d3…   41822                 ");
            output.AppendLine("<missing>           2018-01-03T04:23:16Z        Install update 10.0.16299.192                   100010759                 ");
            output.AppendLine("<missing>           2017-09-29T10:50:38+01:00   Apply image 10.0.16299.15                       203295012                 ");

            var shellExecutor = new Mock<IShellExecutor>();

            shellExecutor
                .Setup(e => e.Execute("docker", " image history -H=False imageName"))
                .Returns(new ShellExecuteResult(true, output.ToString(), ""));

            var dockerImage = new DockerImage("imageName", shellExecutor.Object);

            var results = dockerImage.History().CreateOutputInHumanReadableFormat(false).Execute();

            results.Count.Should().Be(4);
            results[0].ImageId.Should().Be("ecea3d792cd1");
            results[0].CreatedBy.Should().Be("cmd /S /C #(nop)  CMD [\"cmd\" \" / C\" \"type C:\\…");
            results[0].Comment.Should().Be("Comment1");
            results[0].CreatedAt.Should().Be(new DateTime(2018, 1, 8, 20,7, 42));
            results[0].CreatedSince.Should().BeNullOrEmpty();
            results[0].Size.Should().Be("40960");
        }

        [Fact]
        public void WhenImageDoesNotExist_ThenNoResultsAreReturned()
        {
            const string output = "Error response from daemon: No such image: nonExistentImage:latest";

            var shellExecutor = new Mock<IShellExecutor>();

            shellExecutor
                .Setup(e => e.Execute("docker", " image history -H=True nonExistentImage"))
                .Returns(new ShellExecuteResult(true, output.ToString(), ""));

            var dockerImage = new DockerImage("nonExistentImage", shellExecutor.Object);

            var results = dockerImage.History().Execute();

            results.Count.Should().Be(0);
        }
    }
}