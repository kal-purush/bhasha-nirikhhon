using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GestionNote.Classes
{
    class Nav
    {
        private static Nav _instance;

        public static Nav GetInstance()
        {
            if (_instance == null)
            {
                _instance = new Nav();
            }
            return _instance;
        }

        private Nav() { }

        public void GoTo(ViewEnum view)
        {
            OnChangeView(new ChangeViewEventArgs() { View = view });
        }

        protected virtual void OnChangeView(ChangeViewEventArgs e)
        {
            ChangeView?.Invoke(this, e);
        }

        public event ChangeViewEventHandler ChangeView; 
    }

    public class ChangeViewEventArgs : EventArgs
    {
        public ViewEnum View { get; set; }

    }

    public delegate void ChangeViewEventHandler(Object sender, ChangeViewEventArgs e);
}