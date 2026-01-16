using Renci.SshNet;

namespace VPSMonitor.Core.Infrastructure;

public class SftpConnectionContext
{
    public static SftpClient Connect(string host, string username, string password)
    {
        var connectionInfo = new ConnectionInfo(host, username, new PasswordAuthenticationMethod(username, password));
        var sftpClient = new SftpClient(connectionInfo);

        sftpClient.Connect();
        return sftpClient;
    }

    public static void Disconnect(SftpClient client)
    {
        client.Disconnect();
        client.Dispose();
    }
}