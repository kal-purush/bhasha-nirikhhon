namespace Snailer.GodotCSharp.SettingsManager;

using System.Reflection;

public static class DirectoryHelper
{
    public static string ExecutingAssemblyDirectory
    {
        get
        {
            var codeBase = Assembly.GetExecutingAssembly().Location;
            var uri = new UriBuilder(codeBase);
            var path = Uri.UnescapeDataString(uri.Path);

            return Path.GetDirectoryName(path) ?? throw new InvalidOperationException("Failed to locate executing directory.");
        }
    }
}