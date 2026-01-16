using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ProjectFarmerAPI
{
    public class Constants
    {
        #region string constants
        // Items related sprocs
        private static string _itemSaveSprocString = "";
        private static string _itemGetSprocString = "";
        

        // Ratings related sprocs string


        
        // Customers related sprocs string


        
        // Farmers related sprocs string



        // Orders related sprocs string

        #endregion

        #region public string variables
        public static string ItemSaveSprocString { get { return _itemSaveSprocString; } }
        public static string ItemGetSprocString { get { return _itemGetSprocString; } }
        #endregion

    }
}