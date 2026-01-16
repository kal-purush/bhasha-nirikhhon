using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Publishing;
using MQTTnet.Client.Subscribing;
using MQTTnet.Diagnostics;
using MQTTnet.Protocol;
using System;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

namespace Rido.IoTHubClient
{
    public class HubBrokerMqttClient
    {
        const int refreshTokenInterval = 3540000; //59 mins

        public bool IsConnected => mqttClient.IsConnected;
        public event EventHandler<MqttApplicationMessageReceivedEventArgs> OnMessageReceived;
        public event EventHandler<CommandEventArgs> OnCommandReceived;
        public event EventHandler<PropertyEventArgs> OnPropertyReceived;
        public DeviceConnectionString DeviceConnectionString;

        public IMqttClient mqttClient;
        static Timer timerTokenRenew;

        static Action<string> twin_cb;
        static Action<int> patch_cb;
        int lastRid = 1;
        bool reconnecting = false;
        X509Certificate cert;

        private HubBrokerMqttClient()
        {
            MqttNetLogger logger = new MqttNetLogger();
            logger.LogMessagePublished += (s, e) =>
            {
                var trace = $">> [{e.LogMessage.Timestamp:O}] [{e.LogMessage.ThreadId}]: {e.LogMessage.Message}";
                if (e.LogMessage.Exception != null)
                {
                    trace += Environment.NewLine + e.LogMessage.Exception.ToString();
                }

                Trace.TraceInformation(trace);
            };
            mqttClient = new MqttFactory(logger).CreateMqttClient();
        }

        public static async Task<HubBrokerMqttClient> CreateWithClientCertsAsync(string hostname, string certPath, string certPwd)
        {
            using var cert = new X509Certificate2(certPath, certPwd);
            Trace.TraceInformation($"{cert.SubjectName.Name} issued by {cert.IssuerName.Name} NotAfter {cert.GetExpirationDateString()} ({cert.Thumbprint})");
            var cid = cert.Subject.Substring(3);

            var hub = new HubBrokerMqttClient();
            hub.cert = new X509Certificate2(certPath, certPwd);
            ConfigureReservedTopics(hub);
            await hub.mqttClient.ConnectV2WithX509Async(hostname, cert);
            hub.DeviceConnectionString = new DeviceConnectionString($"HostName={hostname};DeviceId={cid};Auth=X509");
            return hub;
        }

        public static async Task<HubBrokerMqttClient> CreateFromConnectionStringAsync(string connectionString) =>
            await CreateFromDCSAsync(new DeviceConnectionString(connectionString));

        public static async Task<HubBrokerMqttClient> CreateAsync(string hostName, string deviceId, string sasKey) =>
            await CreateFromDCSAsync(new DeviceConnectionString() { DeviceId = deviceId, HostName = hostName, SharedAccessKey = sasKey });

        private static async Task<HubBrokerMqttClient> CreateFromDCSAsync(DeviceConnectionString dcs)
        {
            var hub = new HubBrokerMqttClient();
            hub.DeviceConnectionString = dcs;
            ConfigureReservedTopics(hub);
            await hub.mqttClient.ConnectV2WithSasAsync(dcs.HostName, dcs.DeviceId, dcs.SharedAccessKey, 60);
            timerTokenRenew = new Timer(hub.ReconnectWithToken, null, refreshTokenInterval, 0);
            return hub;
        }

        async Task Close()
        {
            var unsuback = await mqttClient.UnsubscribeAsync(new string[]
            {
                "$az/iot/methods/+/+",
                "$az/iot/twin/get/response/+",
                "$az/iot/twin/patch/response/+",
                "$az/iot/twin/events/desired-changed/+"
            });
            unsuback.Items.ToList().ForEach(i => Trace.TraceInformation($"- {i.TopicFilter} {i.ReasonCode}"));
            await mqttClient.DisconnectAsync();
        }

        void ReconnectWithToken(object state)
        {
            lock (this)
            {
                reconnecting = true;
                Trace.TraceWarning("*** REFRESHING TOKEN *** ");
                timerTokenRenew.Dispose();
                Close().Wait();
                var dcs = DeviceConnectionString;
                mqttClient.ConnectWithSasAsync(dcs.HostName, dcs.DeviceId, dcs.SharedAccessKey, 60).Wait();
                reconnecting = false;
                timerTokenRenew = new Timer(ReconnectWithToken, null, refreshTokenInterval, 0);
            }
        }

        static void ConfigureReservedTopics(HubBrokerMqttClient hub)
        {
            hub.mqttClient.UseConnectedHandler(async e =>
            {
                Trace.TraceWarning("### CONNECTED WITH SERVER ###");
                var subres = await hub.mqttClient.SubscribeAsync(new MqttClientSubscribeOptionsBuilder()
                                                        .WithTopicFilter("$az/iot/methods/+/+", MqttQualityOfServiceLevel.AtLeastOnce)
                                                        .WithTopicFilter("$az/iot/twin/get/response/+", MqttQualityOfServiceLevel.AtLeastOnce)
                                                        .WithTopicFilter("$az/iot/twin/patch/response/+", MqttQualityOfServiceLevel.AtLeastOnce)
                                                        .WithTopicFilter("$az/iot/twin/events/desired-changed/+", MqttQualityOfServiceLevel.AtLeastOnce)
                                                        .Build());
                subres.Items.ToList().ForEach(x => Trace.TraceInformation($"+ {x.TopicFilter.Topic} {x.ResultCode}"));
            });

            hub.mqttClient.UseDisconnectedHandler(e =>
            {
                Trace.TraceError("## DISCONNECT ##");
                Trace.TraceError($"** {e.ClientWasConnected} {e.Reason}");
            });

            hub.mqttClient.UseApplicationMessageReceivedHandler(e =>
            {
                string msg = string.Empty;

                var segments = e.ApplicationMessage.Topic.Split('/');
                int rid = 0;
                int twinVersion = 0;
                if (e.ApplicationMessage.Topic.Contains("?"))
                {
                    // parse qs to extract the rid
                    var qs = HttpUtility.ParseQueryString(segments[segments.Length - 1]);
                    rid = Convert.ToInt32(qs["rid"]);
                    twinVersion = Convert.ToInt32(qs["v"]);
                }

                if (e.ApplicationMessage.Payload != null)
                {
                    msg = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);
                }

                //Trace.TraceWarning($"<- {e.ApplicationMessage.Topic}  {e.ApplicationMessage.Payload?.Length} Bytes");
                if (e.ApplicationMessage.Topic.StartsWith("$az/iot/twin/get/response"))
                {
                    twin_cb(msg);
                }
                else if (e.ApplicationMessage.Topic.StartsWith("$az/iot/twin/patch/response/"))
                {
                    patch_cb(twinVersion);
                }
                else if (e.ApplicationMessage.Topic.StartsWith("$az/iot/twin/events/desired-changed"))
                {
                    hub.OnPropertyReceived?.Invoke(hub, new PropertyEventArgs()
                    {
                        Topic = e.ApplicationMessage.Topic,
                        Rid = rid.ToString(),
                        PropertyMessageJson = TwinProperties.RemoveVersion(msg),
                        Version = twinVersion
                    });
                }
                else if (e.ApplicationMessage.Topic.StartsWith("$az/iot/methods"))
                {
                    var cmdName = segments[3];
                    //Trace.TraceWarning($"<- {e.ApplicationMessage.Topic} {cmdName} {e.ApplicationMessage.Payload.Length} Bytes");
                    hub.OnCommandReceived?.Invoke(hub, new CommandEventArgs()
                    {
                        Topic = e.ApplicationMessage.Topic,
                        Rid = rid.ToString(),
                        CommandName = cmdName,
                        CommandRequestMessageJson = msg
                    });
                }
                else
                {
                    hub.OnMessageReceived?.Invoke(hub, e);
                }
            });
        }
        public async Task<MqttClientPublishResult> SendTelemetryAsync(object payload) =>
            await PublishAsync("$az/iot/telemetry", payload);

        public async Task CommandResponseAsync(string rid, string cmdName, string status, object payload) =>
          await PublishAsync($"$iothub/methods/res/{status}/?$rid={rid}", payload);

        public async Task<string> GetTwinAsync()
        {
            var tcs = new TaskCompletionSource<string>();
            var puback = await PublishAsync($"$az/iot/twin/get/?rid={lastRid++}", string.Empty);
            if (puback?.ReasonCode == MqttClientPublishReasonCode.Success)
            {
                twin_cb = s => tcs.TrySetResult(s);
            }
            else
            {
                twin_cb = s => tcs.TrySetException(new ApplicationException($"Error '{puback.ReasonCode}' publishing twin GET: {s}"));
            }
            return tcs.Task.Result;
        }

        public async Task<int> UpdateTwinAsync(object payload)
        {
            var tcs = new TaskCompletionSource<int>();
            var puback = await PublishAsync($"$az/iot/twin/patch/reported/?rid={lastRid++}", payload);
            if (puback?.ReasonCode == MqttClientPublishReasonCode.Success)
            {
                patch_cb = s => tcs.TrySetResult(s);
            }
            else
            {
                patch_cb = s => tcs.TrySetException(new ApplicationException($"Error '{puback.ReasonCode}' publishing twin PATCH: {s}"));
            }
            return tcs.Task.Result;
        }
        public async Task<MqttClientPublishResult> PublishAsync(string topic, object payload)
        {
            while (!mqttClient.IsConnected && reconnecting)
            {
                Trace.TraceWarning(" !! waiting 100 ms to publish ");
                await Task.Delay(100);
            }

            string jsonPayload;
            if (payload is string)
            {
                jsonPayload = payload as string;
            }
            else
            {
                jsonPayload = JsonSerializer.Serialize(payload);
            }
            var message = new MqttApplicationMessageBuilder()
                              .WithTopic(topic)
                              .WithPayload(jsonPayload)
                              .Build();

            MqttClientPublishResult pubRes;
            if (mqttClient.IsConnected)
            {
                try
                {
                    pubRes = await mqttClient.PublishAsync(message, CancellationToken.None);
                    if (pubRes.ReasonCode != MqttClientPublishReasonCode.Success)
                    {
                        Trace.TraceError(pubRes.ReasonCode + pubRes.ReasonString);
                    }
                    //Trace.TraceInformation($"-> {topic} {message.Payload?.Length} Bytes {pubRes.ReasonCode}");
                    return pubRes;
                }
                catch (Exception ex)
                {
                    Trace.TraceError($"Connected?: {mqttClient.IsConnected} Reconnecting?:{reconnecting}");
                    Trace.TraceError(" !!!!!  Failed one message " + ex);
                    return null;
                }
            }
            else
            {
                Trace.TraceWarning(" !!!!!  Missing one message ");
                // TODO: Reconnect here?
                return null;
            }
        }
    }
}