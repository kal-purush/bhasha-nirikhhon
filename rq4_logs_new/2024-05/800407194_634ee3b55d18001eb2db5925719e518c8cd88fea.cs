using FireSharp.Config;
using FireSharp.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;
using winform_chat;

namespace ChessAI_Bck
{
    internal class LoadUserData
    {
        private User GetUser;
        private IFirebaseConfig config = new FirebaseConfig
        {
            AuthSecret = "RZxEKkX6ffq8XZgw9p0jbPYhqLYXQOeH1FIcmGIa",
            BasePath = "https://chess-database-a25f7-default-rtdb.asia-southeast1.firebasedatabase.app/"
        };
        private IFirebaseClient Client;

        private string EncodeSha256(string input)
        {
            byte[] inputBytes = Encoding.UTF8.GetBytes(input);
            byte[] inputHashByte = SHA256.Create().ComputeHash(inputBytes);
            string result = BitConverter.ToString(inputHashByte).Replace("-", string.Empty).ToLower();
            return result;
        }
        public User GetUserData(string username)
        {
            Client = new FireSharp.FirebaseClient(config);
            if (Client != null)
            {
                var find_user = Client.Get("Users/" + EncodeSha256(username));
                if (find_user != null)
                {
                    GetUser = find_user.ResultAs<User>();
                    return GetUser;
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }
    }
}