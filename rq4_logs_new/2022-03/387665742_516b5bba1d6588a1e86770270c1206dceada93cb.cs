using System;
using System.Reflection;

/// <summary>
/// Attribute to denote the project root directory.
/// </summary>
[AttributeUsage(AttributeTargets.Assembly)]
internal class DotnetSdkVersionAttribute : Attribute
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DotnetSdkVersionAttribute" /> class.
    /// </summary>
    /// <param name="version">The .NET SDK version being used.</param>
    public DotnetSdkVersionAttribute(string version)
    {
        Version = version;
    }

    /// <summary>
    /// Gets the project root directory for the executing assembly.
    /// </summary>
    public static string ThisAssemblyDotnetSdkVersion
    {
        get
        {
            var thisAssembly = Assembly.GetExecutingAssembly();
            var attribute = thisAssembly.GetCustomAttribute<DotnetSdkVersionAttribute>();
            return attribute!.Version;
        }
    }

    /// <summary>
    /// Gets the dotnet sdk version.
    /// </summary>
    private string Version { get; }
}