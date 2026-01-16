using Dbosoft.OVN.OSCommands;

namespace Dbosoft.OVN;

/// <summary>
/// Abstraction of the operating system.
/// </summary>
public interface ISystemEnvironment
{
    /// <summary>
    /// Provides an abstraction of the file system.
    /// </summary>
    IFileSystem FileSystem { get; }

    /// <summary>
    /// Creates a new process or attaches to an existing
    /// process when the <paramref name="processId"/> is
    /// not <c>0</c>.
    /// </summary>
    IProcess CreateProcess(int processId = 0);

    /// <summary>
    /// Returns a service manager for the service with
    /// the given <paramref name="serviceName"/>.
    /// </summary>
    IServiceManager GetServiceManager(string serviceName);

    /// <summary>
    /// Returns an OVS extension manager which can be used
    /// to check the availability of the OVS kernel extensions.
    /// </summary>
    IOvsExtensionManager GetOvsExtensionManager();
}