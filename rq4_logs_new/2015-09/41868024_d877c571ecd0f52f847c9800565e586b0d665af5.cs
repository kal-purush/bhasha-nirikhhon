using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FZChat.Client.ViewModel.Messages
{
    public class ConnectionChangedMessage
    {
        public string state { get; set; }
        public ConnectionChangedMessage(string entry)
        {
            state = entry;
        }
    }
}