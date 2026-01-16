using GestionNote.Classes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GestionNote.Classes
{
    class Session
    {
        private static Session _instance;

        public static Session GetInstance()
        {
            if (_instance == null)
            {
                _instance = new Session();
            }
            return _instance;
        }

        public User User { get; set; }

        private Session() { }

        protected virtual void GetUserSession(CurrentUserConnectedEventArgs e)
        {
            CurrentUser?.Invoke(this, e);
        }

        public event CurrentUserConnectedEventHandler CurrentUser;
    }

    public class CurrentUserConnectedEventArgs : EventArgs
    {
        public User usr { get; set; }
    }

    public delegate void CurrentUserConnectedEventHandler(Object sender, CurrentUserConnectedEventArgs e);
}