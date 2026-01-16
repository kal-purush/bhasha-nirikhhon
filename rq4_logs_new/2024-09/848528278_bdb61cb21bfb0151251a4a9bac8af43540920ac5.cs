using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sistema_Ferreteria.Controllers
{
    internal class authController
    {
        private static bool auth = false;


        public void login()
        {
            auth = true;
        }

        public bool checkAuth()
        {
            return auth;
        }
    }

    

    
}