using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FZChat.Client.ViewModel
{

    public class PrivateChat : Chat, INotifyPropertyChanged
    {
        private ClientUser remoteUser;

        public ClientUser RemoteUser
        {
            get { return remoteUser; }
            set
            {
                remoteUser = value;
                OnPropertyChanged("RemoteUser");
            }
        }

        public PrivateChat(ClientUser user) : base()
        {
            remoteUser = user;
        }
    }
}