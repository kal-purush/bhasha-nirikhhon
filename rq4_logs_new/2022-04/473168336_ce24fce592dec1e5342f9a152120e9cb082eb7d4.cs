using OopLab;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OOPLAB_1PreLab
{
    public class UserManager
    {
        private static Users instance = null;
        public UserManager(Users user)
        {
            instance = user;
        }
        public static Users Instance
        {
            get
            {
                if (instance == null)
                {
                    instance = new Users();
                }
                return instance;
            }
        }
    }
}