using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SitecoreX.Framework
{
    static class Constants
    {
        static string _HomeItem = "{110D559F-DEA5-42EA-9C1C-8A5DF7E70EF9}";
        static string _LoginItem = "{2C7828E8-6FC1-4642-BB65-0CEBF2DF919F}";
        static string _RegistrationItem = "{EE995404-8D0F-441E-8D3B-A6A046742967}";
        static public string HomeItem
        {
            get
            {
                return _HomeItem;
            }

        }

        static public string LoginItem
        {
            get
            {
                return _LoginItem;
            }

        }

        static public string RegistrationItem
        {
            get
            {
                return _RegistrationItem;
            }

        }
    }
}