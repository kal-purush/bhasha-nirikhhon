using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;

namespace WCFChat
{
    // HINWEIS: Mit dem Befehl "Umbenennen" im Menü "Umgestalten" können Sie den Klassennamen "Chat" sowohl im Code als auch in der SVC- und der Konfigurationsdatei ändern.
    // HINWEIS: Wählen Sie zum Starten des WCF-Testclients zum Testen dieses Diensts Chat.svc oder Chat.svc.cs im Projektmappen-Explorer aus, und starten Sie das Debuggen.
    [ServiceBehavior(ConcurrencyMode = ConcurrencyMode.Multiple, InstanceContextMode = InstanceContextMode.Single)]
    public class Chat : IChat
    {
        Dictionary<IChatClient, string> users = new Dictionary<IChatClient, string>();
        List<ChatInfos> history = new List<ChatInfos>();
        List<string> userList = new List<string>();

        private Chat() { }

        public List<ChatInfos> GetChats()
        {
            return history;
        }

        public void Join(string name)
        {
            var connection = OperationContext.Current.GetCallbackChannel<IChatClient>();
            users[connection] = name;
            userList.Add(name);
        }

        public void LogOut()
        {
            var connection = OperationContext.Current.GetCallbackChannel<IChatClient>();
            userList.Remove(users[connection]);
            users.Remove(connection);
            Refresh();
        }

        public void Refresh()
        {
            foreach (var user in users.Keys)
            {
                user.RefreshUserList(userList);
            }
        }

        public void SendChat(string name, string text)
        {
            var connection = OperationContext.Current.GetCallbackChannel<IChatClient>();
            ChatInfos newChat = new ChatInfos(name, text, DateTime.Now);
            history.Add(newChat);

            if (!users.TryGetValue(connection, out name))
                return;

            foreach (var user in users.Keys)
            {
                user.ReceiveChat(newChat);
            }
        }
    }
}