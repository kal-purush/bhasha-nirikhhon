#region usings
//vvvv related
using System;
using System.ComponentModel.Composition;
using VVVV.PluginInterfaces.V2;
using VVVV.Core.Logging;

//functionality related
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;
#endregion usings

namespace VVVV.Nodes.MQTT
{
    #region PluginInfo
    [PluginInfo(Name = "Client",
                Category = "Network",
                Version = "MQTT",
                Help = "client to connect to a MQTT broker",
                Tags = "IoT, MQTT", Credits = "M2MQTT m2mqtt.wordpress.com, Jochen Leinberger, explorative-environments.net",
                Author = "woei",
                Bugs = "receiving delete retained message command",
                AutoEvaluate = true)]
    #endregion PluginInfo
    public class MQTTClientNode : MQTTConnection
    {
        #region pins
        [Output("Client")]
        public ISpread<MqttClient> FOutClient;
        #endregion pins

        public override void OnImportsSatisfied()
        {
            FOutClient[0] = null;
            base.OnImportsSatisfied();  
        }

        public override void Evaluate(int spreadMax)
        {
            base.Evaluate(spreadMax);
            if (FOutClient[0] == null && FClient != null)
                FOutClient[0] = FClient;

            if (FOutClient[0] != null && FClient == null)
                FOutClient[0] = null;
        }

        public override void FClient_MqttMsgPublished(object sender, MqttMsgPublishedEventArgs e) {}
        public override void FClient_MqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e) {}
        public override void FClient_MqttMsgSubscribed(object sender, MqttMsgSubscribedEventArgs e) {}
        public override void FClient_MqttMsgUnsubscribed(object sender, MqttMsgUnsubscribedEventArgs e) {}
    }

    #region PluginInfo
    [PluginInfo(Name = "Send",
                Category = "Network",
                Version = "MQTT",
                Help = "Client for communicating via the MQTT protocol",
                Tags = "IoT, MQTT", Credits = "M2MQTT m2mqtt.wordpress.com, Jochen Leinberger, explorative-environments.net",
                Author = "woei",
                Bugs = "receiving delete retained message command",
                AutoEvaluate = true)]
    #endregion PluginInfo
    public class MQTTSendNode : IPluginEvaluate, IPartImportsSatisfiedNotification
    {
        #region pins
        [Input("Client")]
        public Pin<MqttClient> FInClient;

        [Input("Topic", DefaultString = "vvvv/topic")]
        public ISpread<string> FInTopic;

        [Input("Message", DefaultString = "hello vvvv")]
        public ISpread<string> FInMessage;

        [Input("Qualiy of Service", DefaultEnumEntry = "QoS_0")]
        public IDiffSpread<QOS> FInQoS;

        [Input("Retained", IsBang = true)]
        ISpread<bool> FInRetained;

        [Input("Do Send", IsBang = true)]
        ISpread<bool> FInSend;

        [Input("Remove Retained", IsBang = true)]
        ISpread<bool> FInRemoveRetained;

        [Output("Publish Queue Count", Visibility = PinVisibility.OnlyInspector)]
        public ISpread<int> FOutboundCount;

        [Output("Message Status")]
        public ISpread<string> FOutMessageStatus;
        #endregion pins

        #region fields
        [Import()]
        public ILogger FLogger;

        MqttClient FClient = null;
        System.Text.UTF8Encoding UTF8Enc = new System.Text.UTF8Encoding();
        Queue<string> FMessageStatusQueue = new Queue<string>();

        HashSet<ushort> FPublishStatus = new HashSet<ushort>(); //keeps track received-confirmations of published packets
        #endregion fields

        public void OnImportsSatisfied()
        {
            FInClient.Connected += FInClient_Connected;
            FInClient.Disconnected += FInClient_Disconnected;
        }

        #region events
        private void FInClient_Connected(object sender, PinConnectionEventArgs args)
        {
            if (FInClient[0] != null)
            {
                FClient = FInClient[0];
                FClient.MqttMsgPublished += FClient_MqttMsgPublished;
            }
        }

        private void FInClient_Disconnected(object sender, PinConnectionEventArgs args)
        {
            if (FClient != null)
            {
                FClient.MqttMsgPublished -= FClient_MqttMsgPublished;
                FClient = null;
            }
        }

        /// <summary>
        /// confirmation of the broker that a packet was successfully transmitted
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        public void FClient_MqttMsgPublished(object sender, MqttMsgPublishedEventArgs e)
        {
            if (FPublishStatus.Contains(e.MessageId))
            {
                FMessageStatusQueue.Enqueue("published message with packet ID " + e.MessageId);
                FPublishStatus.Remove(e.MessageId);
            }
        }
        #endregion events

        public void Evaluate (int spreadmax)
        {
            if (FInClient.IsConnected)
            {
                if (FClient == null && FInClient[0] != null)
                {
                    FClient = FInClient[0];
                    FClient.MqttMsgPublished += FClient_MqttMsgPublished;
                }
                if (FClient != null && FInClient[0] == null)
                {
                    FClient.MqttMsgPublished -= FClient_MqttMsgPublished;
                    FClient = null;
                }

                    if (FClient != null && FClient.IsConnected)
                {
                    for (int i = 0; i < spreadmax; i++)
                    {
                        if (FInSend[i])
                        {
                            if (FInTopic[i].Contains("/#") || FInTopic[i].Contains("/*"))
                            {
                                FMessageStatusQueue.Enqueue("Topic at slice " + i.ToString() + " contains illegal characters for publishing");
                            }
                            else
                            {
                                var packetId = FClient.Publish(FInTopic[i], UTF8Enc.GetBytes(FInMessage[i]), (byte)FInQoS[i], FInRetained[i]);
                                FPublishStatus.Add(packetId);
                            }

                        }
                        if (FInRemoveRetained[i])
                            FClient.Publish(FInTopic[i], new byte[] { }, (byte)0, true);
                    }
                }
            }

            if(FMessageStatusQueue.Count > 0)
            {
                FOutMessageStatus.AssignFrom(FMessageStatusQueue.ToArray());
                FMessageStatusQueue.Clear();
            }
        }
    }

    #region PluginInfo
    [PluginInfo(Name = "Receive",
                Category = "Network",
                Version = "MQTT",
                Help = "Client for communicating via the MQTT protocol",
                Tags = "IoT, MQTT", Credits = "M2MQTT m2mqtt.wordpress.com, Jochen Leinberger, explorative-environments.net",
                Author = "woei",
                Bugs = "receiving delete retained message command",
                AutoEvaluate = true)]
    #endregion PluginInfo
    public class MQTTReceiveNode : IPluginEvaluate, IPartImportsSatisfiedNotification
    {
        #region pins
        [Input("Client")]
        public Pin<MqttClient> FInClient;

        [Input("Topic", DefaultString = "vvvv/topic")]
        public ISpread<string> FInTopic;

        [Input("Qualiy of Service", DefaultEnumEntry = "QoS_0")]
        public IDiffSpread<QOS> FInQoS;

        [Output("Topic")]
        public ISpread<string> FOutTopic;

        [Output("Message")]
        public ISpread<string> FOutMessage;

        [Output("Qualiy of Service")]
        public ISpread<QOS> FOutQoS;

        [Output("Is Retained")]
        ISpread<bool> FOutIsRetained;

        [Output("On Data")]
        ISpread<bool> FOutOnData;

        [Output("Message Status")]
        public ISpread<string> FOutMessageStatus;
        #endregion pins

        #region fields
        [Import()]
        public ILogger FLogger;

        MqttClient FClient = null;
        System.Text.UTF8Encoding UTF8Enc = new System.Text.UTF8Encoding();
        Queue<string> FMessageStatusQueue = new Queue<string>();

        Queue<MqttMsgPublishEventArgs> FPacketQueue = new Queue<MqttMsgPublishEventArgs>();  //incoming packets
        HashSet<Tuple<string, QOS>> FSubscriptions = new HashSet<Tuple<string, QOS>>(); //list of current subscriptions
        Dictionary<ushort, Tuple<string, QOS>> FSubscribeStatus = new Dictionary<ushort, Tuple<string, QOS>>(); //matches subscribe commands to packet ids
        Dictionary<ushort, Tuple<string, QOS>> FUnsubscribeStatus = new Dictionary<ushort, Tuple<string, QOS>>(); //matches unsubscribe commands to packet ids
        #endregion fields

        public void OnImportsSatisfied()
        {
            FInClient.Connected += FInClient_Connected;
            FInClient.Disconnected += FInClient_Disconnected;
        }

        #region events
        private void FInClient_Connected(object sender, PinConnectionEventArgs args)
        {
            if (FInClient[0] != null)
            {
                FClient = FInClient[0];
                FClient.MqttMsgPublishReceived += FClient_MqttMsgPublishReceived;
                FClient.MqttMsgSubscribed += FClient_MqttMsgSubscribed;
                FClient.MqttMsgUnsubscribed += FClient_MqttMsgUnsubscribed;
            }
        }

        private void FInClient_Disconnected(object sender, PinConnectionEventArgs args)
        {
            if (FClient != null)
            {
                FClient.MqttMsgPublishReceived -= FClient_MqttMsgPublishReceived;
                FClient.MqttMsgSubscribed -= FClient_MqttMsgSubscribed;
                FClient.MqttMsgUnsubscribed -= FClient_MqttMsgUnsubscribed;
                FClient = null;
            }
        }

        /// <summary>
        /// passes packets received by the mqtt-client
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e">packet data</param>
        public void FClient_MqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e)
        {
            bool match = false;
            foreach (var t in FInTopic)
                if (MatchTopic(t, e.Topic))
                    match = true;
            if (match)
            {
                FPacketQueue.Enqueue(e);
                FMessageStatusQueue.Enqueue("received topic " + e.Topic);
            }
        }

        /// <summary>
        /// broker acknowledges a subscription
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        public void FClient_MqttMsgSubscribed(object sender, MqttMsgSubscribedEventArgs e)
        {
            if (FSubscribeStatus.ContainsKey(e.MessageId))
            {
                var issued = FSubscribeStatus[e.MessageId];
                FMessageStatusQueue.Enqueue("subscribed to " + issued.Item1);
                FSubscribeStatus.Remove(e.MessageId);
            }
        }

        /// <summary>
        /// broker acknowledges an unsubscription
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        public void FClient_MqttMsgUnsubscribed(object sender, MqttMsgUnsubscribedEventArgs e)
        {
            if (FUnsubscribeStatus.ContainsKey(e.MessageId))
            {
                var issued = FUnsubscribeStatus[e.MessageId];
                FMessageStatusQueue.Enqueue("unsubscribed from " + issued.Item1);
                FUnsubscribeStatus.Remove(e.MessageId);
            }
        }
        #endregion events

        /// <summary>
        /// matches incoming topics with requested ones with possible wildcards
        /// </summary>
        /// <param name="request">requested topic string</param>
        /// <param name="response">received topic string</param>
        /// <returns></returns>
        bool MatchTopic(string request, string response)
        {
            var _src = request.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            var _dst = response.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            bool match = false;
            for (int i=0; i<Math.Max(_src.Length,_dst.Length); i++)
            {
                if ((i>=_src.Length) || (i >= _dst.Length))
                {
                    match = false;
                    break;
                }
                else if (_src[i] == "+")
                    match = true;
                else if (_src[i] == _dst[i])
                    match = true;
                else if (_src[i] == "#")
                {
                    match = true;
                    break;
                }
                else
                    match = false;

                if (!match)
                    break;
            }
            return match;
        }

        public void Evaluate(int spreadmax)
        {
            if (FInClient.IsConnected)
            {
                if (FClient == null && FInClient[0] != null)
                {
                    FClient = FInClient[0];
                    FClient.MqttMsgPublishReceived += FClient_MqttMsgPublishReceived;
                    FClient.MqttMsgSubscribed += FClient_MqttMsgSubscribed;
                    FClient.MqttMsgUnsubscribed += FClient_MqttMsgUnsubscribed;
                }
                if (FClient != null && FInClient[0] == null)
                {
                    FClient.MqttMsgPublishReceived -= FClient_MqttMsgPublishReceived;
                    FClient.MqttMsgSubscribed -= FClient_MqttMsgSubscribed;
                    FClient.MqttMsgUnsubscribed -= FClient_MqttMsgUnsubscribed;
                    FClient = null;
                }

                if (FClient != null && FClient.IsConnected)
                {
                    HashSet<Tuple<string, QOS>> currentSubscriptions = new HashSet<Tuple<string, QOS>>();
                    List<Tuple<string, QOS>> newSubscriptions = new List<Tuple<string, QOS>>();
                    for (int i = 0; i < spreadmax; i++)
                    {
                        var tup = new Tuple<string, QOS>(FInTopic[i], FInQoS[i]);
                        currentSubscriptions.Add(tup);

                        if (!FSubscriptions.Remove(tup))
                            newSubscriptions.Add(tup);
                    }

                    #region unsubscribe
                    try
                    {
                        if (FSubscriptions.Count > 0)
                        {
                            foreach (var tuple in FSubscriptions)
                            {
                                var unsubscribeId = FClient.Unsubscribe(new string[] { tuple.Item1 });
                                FUnsubscribeStatus.Add(unsubscribeId, tuple);
                            }
                        }
                        FSubscriptions = new HashSet<Tuple<string, QOS>>(currentSubscriptions);
                    }
                    catch (Exception e)
                    {
                        FLogger.Log(e);
                        foreach (var s in currentSubscriptions)
                            FSubscriptions.Add(s);
                    }
                    #endregion unsubscribe

                    #region subscribe
                    foreach (var subs in newSubscriptions)
                    {
                        try
                        {
                            var subscribeId = FClient.Subscribe(new string[] { subs.Item1 }, new byte[] { (byte)subs.Item2 });
                            FSubscribeStatus.Add(subscribeId, subs);
                        }
                        catch
                        {
                            FLogger.Log(LogType.Warning, string.Format("couldn't subscribe to {0} with qos {1}", subs.Item1, subs.Item2));
                        }
                    }
                    #endregion subscribe
                }
            }

            FOutTopic.AssignFrom(FPacketQueue.Select(x => x.Topic).ToArray());
            FOutMessage.AssignFrom(FPacketQueue.Select(x => UTF8Enc.GetString(x.Message)));
            FOutQoS.AssignFrom(FPacketQueue.Select(x => (QOS)x.QosLevel));
            FOutIsRetained.AssignFrom(FPacketQueue.Select(x => x.Retain));
            FOutOnData[0] = FPacketQueue.Count > 0;
            FPacketQueue.Clear();

            if (FMessageStatusQueue.Count > 0)
            {
                FOutMessageStatus.AssignFrom(FMessageStatusQueue.ToArray());
                FMessageStatusQueue.Clear();
            }
        }
    }
}