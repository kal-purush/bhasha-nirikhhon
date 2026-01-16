using OpenFin.Net.Adapter.Channels;
using OpenFin.Net.Adapter;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic.Logging;

namespace SideCar.App
{
    internal class Provider
    {
        private IRuntime? runtime;
        private IChannelProvider? provider;
        private IChannels? channels;
        private Action<string>? onAction;

        private const string SIDECAR_CHANNEL_NAME = "sidecar-app";
        private const string SIDECAR_CLIENT_FUNCTION_NAME = "sidecar-app-client-subscriber";
        private const string SIDECAR_CHANNEL_FUNCTION_NAME = "sidecar-app-echo";

        public async void Init(Action<string> onProviderAction)
        {
            onAction = onProviderAction;
            runtime = new RuntimeFactory()
               .GetRuntimeInstance(new RuntimeOptions
               {
                   Version = "32.114.76.14",
                   UUID = "sidecar-app",
                   LicenseKey = "openfin-demo-license-key"
               });

            onAction("Connected to the runtime.");

            runtime.Connected += Runtime_Connected;
            runtime.Disconnected += Runtime_Disconnected;

            await runtime.ConnectAsync();

            channels = runtime.GetService<IChannels>();

            onAction("Creating Provider");
            provider = channels.CreateProvider(SIDECAR_CHANNEL_NAME);
            onAction("Registering echo topic.");
            provider.RegisterTopic<string, string>(SIDECAR_CHANNEL_FUNCTION_NAME, (payload) =>
            {
                onAction("Message received on SideCar App from connected client. Responding with Echo prefixed to recieved message.");
                return "echo: " + payload;
            });
            provider.Opened += (s, e) =>
            {
                onAction("Provider opened.");
            };

            provider.Closed += (s, e) =>
            {
                onAction("Provider closed.");
            };

            provider.ClientConnected += (s, e) =>
            {
                onAction("Client Connected");
                if(e.Payload != null)
                {
                    onAction("Client Payload: " + e.Payload.ToJsonString());
                }

                if(e.Client?.RemoteEndpoint?.Uuid != null)
                {
                    onAction("Client UUID: " + e.Client.RemoteEndpoint.Uuid);
                }
            };

            provider.ClientDisconnected += (s, e) =>
            {
                onAction("Client Disconnected");
            };
            await provider.OpenAsync();
        }

        public async void BroadcastMessage(string message)
        {
            if(provider != null)
            {
                await provider.BroadcastAsync(SIDECAR_CLIENT_FUNCTION_NAME, message);
            }
            if(onAction != null)
            {
                onAction("Provider has broadcast message: " + message);
            }
        }


        private void Runtime_Connected(object? sender, EventArgs e)
        {
            if(onAction != null)
            {
                onAction("Runtime connected.");
            }
        }

        private void Runtime_Disconnected(object? sender, EventArgs e)
        {
            if (onAction != null)
            {
                onAction("Runtime disconnected.");
            }
        }

    }
}