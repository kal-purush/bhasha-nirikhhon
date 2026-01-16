using MQTTnet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rido.IoTHubClient;

namespace sample_device
{
    class Program_BYOM
    {

        static string DefaultKey  => Convert.ToBase64String(Encoding.UTF8.GetBytes(Guid.Empty.ToString("N")));

        public static async Task Main_BYO(string[] args)
        {
            MQTTnet.Client.IMqttClient mqttClient = new MqttFactory().CreateMqttClient();

            var connack= await mqttClient.ConnectV2WithSasAsync("broker.azure-devices.net", "d4", DefaultKey);
            
            Console.WriteLine(connack.ResultCode);
            Console.WriteLine(connack.MaximumQoS);
            Console.WriteLine(connack.IsSessionPresent);
        }
    }
}