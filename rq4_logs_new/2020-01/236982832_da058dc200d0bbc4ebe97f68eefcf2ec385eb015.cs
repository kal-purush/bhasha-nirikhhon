using System;
using System.Collections.Generic;
using System.Text;

namespace netBot
{
    abstract class ChatMessageHandler : IChatMessageHandler
    {
        string IChatMessageHandler.Handle(string message)
        {
            throw new NotImplementedException();
        }
    }
}