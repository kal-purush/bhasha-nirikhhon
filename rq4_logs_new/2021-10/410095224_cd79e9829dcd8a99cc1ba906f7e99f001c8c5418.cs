using MQTTnet.Client.Publishing;
using System;
using System.Threading.Tasks;

namespace Rido.IoTHubClient
{
    public interface IHubMqttClient
    {
        bool IsConnected { get; }
        DeviceConnectionString DeviceConnectionString { get; }
        Task<MqttClientPublishResult> SendTelemetryAsync(object payload);
        Task<string> GetTwinAsync();
        Task<int> UpdateTwinAsync(object payload);
        event EventHandler<PropertyEventArgs> OnPropertyReceived;
        event EventHandler<CommandEventArgs> OnCommandReceived;
        Task CommandResponseAsync(string rid, string cmdName, string status, object payload);
        Task CloseAsync();
    }
}