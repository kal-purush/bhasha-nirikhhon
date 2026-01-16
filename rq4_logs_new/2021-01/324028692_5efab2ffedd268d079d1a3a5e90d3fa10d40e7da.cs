using System;
using MultiWorldProtocol.Messaging;
using MultiWorldProtocol.Messaging.Definitions;

namespace MultiWorldProtocol.Messaging.Definitions.Messages
{
    [MWMessageType(MWMessageType.SaveMessage)]
    public class MWSaveMessage : MWMessage
    {
        public MWSaveMessage()
        {
            MessageType = MWMessageType.SaveMessage;
        }
    }

    public class MWSaveMessageDefinition : MWMessageDefinition<MWSaveMessage>
    {
        public MWSaveMessageDefinition() : base(MWMessageType.SaveMessage)
        {
        }
    }
}