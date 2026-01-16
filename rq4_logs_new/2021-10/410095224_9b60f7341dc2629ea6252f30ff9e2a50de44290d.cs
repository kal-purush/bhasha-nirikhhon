using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Connecting;
using Rido.IoTHubClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Rido.IoTHubClient.Tests
{
    public class IMqttClientExtensionsFixture : IDisposable
    {
        string hostname = "broker.azure-devices.net";
        string deviceId = "d5";
        string DefaultKey => Convert.ToBase64String(Encoding.UTF8.GetBytes(Guid.Empty.ToString("N")));
        IMqttClient mqttClient;
        public IMqttClientExtensionsFixture()
        {
            mqttClient = new MqttFactory().CreateMqttClient();
        }

        [Fact]
        public async Task ConnectWithSaSV2()
        {
            var connack = await mqttClient.ConnectV2WithSasAsync(hostname, deviceId, DefaultKey);
            Assert.Equal(MqttClientConnectResultCode.Success, connack.ResultCode);
        }
        [Fact]
        public async Task ConnectWithSaSV1()
        {
            var connack = await mqttClient.ConnectWithSasAsync(hostname, deviceId, DefaultKey);
            Assert.Equal(MqttClientConnectResultCode.Success, connack.ResultCode);
        }

        [Fact]
        public async Task ConnectWithCertsV1()
        {
            var connack = await mqttClient.ConnectWithX509Async(hostname, new X509Certificate("testdevice.pfx", "1234"));
            Assert.Equal(MqttClientConnectResultCode.Success, connack.ResultCode);
        }

        [Fact]
        public async Task ConnectWithCertsV2()
        {
            var connack = await mqttClient.ConnectV2WithX509Async(hostname, new X509Certificate("testdevice.pfx", "1234"));
            Assert.Equal(MqttClientConnectResultCode.Success, connack.ResultCode);
        }

        public void Dispose()
        {
            _ = mqttClient.DisconnectAsync();
        }
    }
}