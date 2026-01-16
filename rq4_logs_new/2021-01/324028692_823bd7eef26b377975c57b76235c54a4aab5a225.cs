using System;
using MultiWorldProtocol.Messaging;
using MultiWorldProtocol.Messaging.Definitions;

namespace MultiWorldProtocol.Messaging.Definitions.Messages
{
    [MWMessageType(MWMessageType.NumReadyMessage)]
    public class MWNumReadyMessage : MWMessage
    {
        public int Ready { get; set; }

        public MWNumReadyMessage()
        {
            MessageType = MWMessageType.NumReadyMessage;
        }
    }

    public class MWNumReadyMessageDefinition : MWMessageDefinition<MWNumReadyMessage>
    {
        public MWNumReadyMessageDefinition() : base(MWMessageType.NumReadyMessage)
        {
            Properties.Add(new MWMessageProperty<int, MWNumReadyMessage>(nameof(MWNumReadyMessage.Ready)));
        }
    }
}