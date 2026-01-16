using LangSharp.Core.Interfaces.Services;
using LangSharp.Utils;
using System.Runtime.InteropServices;

namespace LangSharp.Core.Services
{
    public class PathService : IPathService
    {
        public string GetPythonDllPath()
        {
            return Path.Combine(GetPythonPath(), EnvironmentConsts.DllVersionName);
        }

        public string GetPythonPath()
        {
            return Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                ".nuget", "packages", "python", EnvironmentConsts.PythonVersion, "tools");
        }

        public string GetPythonPathExecutable()
        {
            var pythonHome = Environment.GetEnvironmentVariable("PYTHONHOME", EnvironmentVariableTarget.Process);

            if (string.IsNullOrEmpty(pythonHome))
                return string.Empty;

            var isVirtualEnv = pythonHome.EndsWith(EnvironmentConsts.VirtualEnvironment, StringComparison.OrdinalIgnoreCase);

            return isVirtualEnv
                ? GetPythonPathExecutableForVenv(pythonHome)
                : GetPythonPathExecutableForStandardInstallation(pythonHome);
        }

        public string GetScriptsPath(string scriptName)
        {
            return Path.Combine(AppContext.BaseDirectory, "scripts", scriptName);
        }

        public string GetScriptsPathByPackageDir(string scriptName)
        {
            return Path.Combine(
              Path.Combine(
                  Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                  ".nuget", "packages", "langsharp", EnvironmentConsts.GetLangSharpAssemblyVersion()
              ),
              "Scripts", scriptName);
        }

        public string GetSitePackagesPath(string basePath)
        {
            return Path.Combine(basePath, "Lib", "site-packages");
        }

        public string GetSitePackagesPathFromPythonHome()
        {
            var pythonHome = Environment.GetEnvironmentVariable("PYTHONHOME", EnvironmentVariableTarget.Process);

            if (string.IsNullOrEmpty(pythonHome))
                return string.Empty;

            var isVirtualEnv = pythonHome.EndsWith(EnvironmentConsts.VirtualEnvironment, StringComparison.OrdinalIgnoreCase);

            return isVirtualEnv
                ? Path.Combine(pythonHome, "Lib", "site-packages")
                : Path.Combine(pythonHome, "Lib");
        }

        public string GetVenvPath()
        {
            return Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                ".nuget", "packages", "python", EnvironmentConsts.PythonVersion,
                EnvironmentConsts.VirtualEnvironment);
        }

        public string GetDirectoryName(string? path)
        {
            return Path.GetDirectoryName(path) ?? string.Empty;
        }

        private static string GetPythonPathExecutableForVenv(string pythonHome)
        {
            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows) switch
            {
                true => Path.Combine(pythonHome, "Scripts", "python.exe"),
                false when RuntimeInformation.IsOSPlatform(OSPlatform.Linux) => Path.Combine(pythonHome, "bin", "python"),
                _ => throw new PlatformNotSupportedException("Unsupported operating system.")
            };
        }

        private static string GetPythonPathExecutableForStandardInstallation(string pythonHome)
        {
            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows) switch
            {
                true => Path.Combine(pythonHome, "python.exe"),
                false when RuntimeInformation.IsOSPlatform(OSPlatform.Linux) => Path.Combine(pythonHome, "bin", "python"),
                _ => throw new PlatformNotSupportedException("Unsupported operating system.")
            };
        }

    }
}