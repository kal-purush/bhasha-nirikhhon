using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FZChat.Model
{
    enum OnlineStatus
    {
        OFFLINE,
        ONLINE
    }
    class ClientUser : IUser
    {
        public string UserName { get; set; }
        public string NickName { get; set; }
        public GenderOption Gender { get; set; }
        public int Age { get; set; }
        public string Email { get; set; }
        public int Head { get; set; }
        public OnlineStatus Status { get; set; }
        public ClientUser()
        {
            Status = OnlineStatus.OFFLINE;
        }
    }
}