using LangSharp.Core.Factorys;
using LangSharp.Core.Services;
using System.Runtime.InteropServices;

namespace LangSharp.UnitTests.Core.Factorys
{
    public class PathServiceFactoryTests
    {
        [Fact]
        public void CreateForCurrentEnvironment_ShouldReturnPathService_WhenWindows()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return; // Skip test if not running on Windows

            var service = PathServiceFactory.CreateForCurrentEnvironment();
            Assert.IsType<PathService>(service);
        }

        [Fact]
        public void CreateForCurrentEnvironment_ShouldReturnPathLinuxService_WhenLinux()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                return; // Skip test if not running on Linux

            var service = PathServiceFactory.CreateForCurrentEnvironment();
            Assert.IsType<PathLinuxService>(service);
        }

    }
}