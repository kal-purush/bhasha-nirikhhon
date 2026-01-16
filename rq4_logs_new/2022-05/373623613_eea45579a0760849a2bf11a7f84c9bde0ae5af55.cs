using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MusicPlayerBackend.Contracts
{
    public interface INotifyUI
    {
        public void Notify(NotificationModel message);
    }
}