using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace VkApp.FileManager
{
    public class FriendsClass
    {
        private const string _filePath = "Friends.txt";
        private const string _fileBot = "BotFriendsFolder\\";
        private List<string> _friendList;

        public FriendsClass()
        {
            _friendList = new List<string>();
        }

        public List<string> GetAllFriendsFromFile(string userLogin, string gameName)
        {
            string path = _fileBot + userLogin + "\\" + gameName + ".txt";
            if (File.Exists(path))
            {
                string[] result = File.ReadAllLines(path);
                int needPos = result.ToList<string>().IndexOf(userLogin);

                while (result.ToList<string>()[needPos].IndexOf(" ") != 0)
                {
                    _friendList.Add(result[needPos]);
                    needPos++;
                }
                return _friendList;
            }
            else
                return null;
        }
        
        public void AddFriendsToFile(string userLogin, string gameName, List<string> friendRequests)
        {
            string path = _fileBot + userLogin + "\\" + gameName + ".txt";
            using (FileStream file = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite))
            {
                using (StreamWriter writer = new StreamWriter(file, Encoding.Default))
                {
                    writer.WriteLine(userLogin);
                    foreach (string str in friendRequests) { writer.WriteLine(str); }
                }
            }

            using (FileStream file = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite))
            {
                using (StreamWriter writer = new StreamWriter(file, Encoding.Default))
                {
                    writer.WriteLine(userLogin);
                    foreach (string str in friendRequests) { writer.WriteLine(str); }
                }
            }
        }
        
        public List<string> GetFriends { get { return _friendList; } }
        
        internal bool IsFriendExist(string text)
        {
            string friend = text;
            friend.Replace('\n', new char());
            friend.Replace('\r', ' ');

            string[] allFriends = File.ReadAllLines(_filePath);
            for (int i = 0; i < allFriends.Count(); i++)
                if (allFriends[i] == friend)
                    return true;            
            return false;
        }
    }
}