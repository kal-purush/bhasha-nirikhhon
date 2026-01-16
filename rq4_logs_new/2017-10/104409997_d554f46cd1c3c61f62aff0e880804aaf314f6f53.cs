using System;
namespace EthereumDemoApp.Models
{
    public class User
    {
        public string Name { get; set; }
        public string Email { get; set; }

        #region Singleton
        private static User instance = null;

        private User()
        {
            
        }

        public static User GetInstance()
        {
            if (instance == null)
            {
                instance = new User();
            }
           
            return instance;
        }

        public static void DeleteInstance()
        {
            if (instance != null)
            {
                instance = null;
            }
        }
        #endregion
         
    }
}