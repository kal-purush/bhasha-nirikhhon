using Rido.IoTHubClient;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sample_device
{
    class ProgramV1Topics
    {
        static async Task MainV1(string[] args)
        {
            //var dpsRes = await DpsClient.ProvisionWithSasAsync("0ne00385995", "paad", "lD9e/S1YjubD2yRUdkzUI/uPME6KP4Es4Ulhh2Kyh1g=");
            //Console.WriteLine(dpsRes.registrationState.assignedHub);
            //var client1 = await HubMqttClient.CreateAsync(dpsRes.registrationState.assignedHub, dpsRes.registrationState.deviceId, "lD9e/S1YjubD2yRUdkzUI/uPME6KP4Es4Ulhh2Kyh1g=");
            //var t1 = await client1.GetTwinAsync();
            //Console.WriteLine("Twin1 REPLY 1" + t1);


            Trace.Listeners[0].Filter = new EventTypeFilter(SourceLevels.Information);
            Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));
            Trace.Listeners[1].Filter = new EventTypeFilter(SourceLevels.Warning);

            //var client = await HubMqttClient.CreateWithClientCertsAsync("rido.azure-devices.net","../../../../.certs/devx1.pfx", "1234");
            var client = await HubMqttClient.CreateFromConnectionStringAsync(Environment.GetEnvironmentVariable("cs"));
            Console.WriteLine();
            Console.WriteLine(client.DeviceConnectionString);
            Console.WriteLine();

            client.OnCommandReceived += async (s, e) =>
            {
                Console.WriteLine($"Processing Command {e.CommandName}");
                await Task.Delay(500);
                await client.CommandResponseAsync(e.Rid, e.CommandName, "200", new { myResponse = "ok" });
            };

            client.OnPropertyReceived += async (s, e) => {
                Console.WriteLine($"Processing Desired Property {e.PropertyMessageJson}");
                await Task.Delay(500);
                // todo parse property
                var ack = TwinProperties.BuildAck(e.PropertyMessageJson, e.Version, 200, "update ok");
                var v = await client.UpdateTwinAsync(ack);
                Console.WriteLine("PATCHED ACK: " + v);
            };

            await Task.Delay(500);
            await client.SendTelemetryAsync(new { temperature = 1 });

            var t = await client.GetTwinAsync();
            Console.WriteLine("Twin REPLY 1" + t);

            var v = await client.UpdateTwinAsync(new { tool = "from mqttnet22 " + System.Environment.TickCount });
            Console.WriteLine("Twin PATCHED version: " + v);

            while (client.IsConnected)
            {
                await client.SendTelemetryAsync(new { temperature = 1 });
                await Task.Delay(5000);
                Console.Write("t");
            }

            await client.SendTelemetryAsync(new { temperature = 1 });


            Console.WriteLine("End");

        }
    }
}