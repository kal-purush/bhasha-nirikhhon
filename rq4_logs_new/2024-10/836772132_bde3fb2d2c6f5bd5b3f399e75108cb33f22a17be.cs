using System.Security.Cryptography.X509Certificates;

namespace Mortein.Mqtt;

/// <summary>
/// Provides authentication for the MQTT service.
/// </summary>
public static class MqttAuthentication
{
    /// <summary>
    /// Return the <see cref="X509Certificate2"/> with which to authenticate to the MQTT broker.
    /// </summary>
    /// 
    /// <returns>The <see cref="X509Certificate2"/> with which to authenticate to the MQTT broker</returns>
    public static X509Certificate2 GetAwsMqttCertificate()
    {
        // TODO: read from somewhere other than the local filesystem.
        return new("/workspaces/api/api.pfx");
    }
}