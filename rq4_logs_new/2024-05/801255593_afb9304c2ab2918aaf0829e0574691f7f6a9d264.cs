using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClasesBase
{
    public class UsuarioLogIn
    {
        private static UsuarioLogIn _instance;
        private Usuario _usuario;

        private UsuarioLogIn() { }

        public static UsuarioLogIn Instance
        {
            get
            {
                if (_instance == null)
                {
                    _instance = new UsuarioLogIn();
                }
                return _instance;
            }
        }

        public Usuario Usuario
        {
            get { return _usuario; }
            set { _usuario = value; }
        }
    
}
}