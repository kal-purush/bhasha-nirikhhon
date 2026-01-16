using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Utilities
{
    public interface IUserLoader
    {
        User Load(string name);
        void Save(User user);
    }

    public class FileUserLoader : IUserLoader
    {
        private string usersFolderPath = AppDomain.CurrentDomain.BaseDirectory + "UsersData\\";

        public User Load(string name)
        {
            var fullPath = usersFolderPath + name + ".txt";
            try
            {
                using (var file = File.OpenRead(fullPath))
                {
                    var array = new byte[file.Length];
                    file.Read(array, 0, array.Length);
                    var textFromFile = Encoding.Default.GetString(array);
                    if (int.TryParse(textFromFile, out int money))
                    {
                        return new User(name, money);
                    }   
                }
            }
            catch
            {
                // Молчаливое сглатывание
            }
            return null;
        }

        public void Save(User user)
        {
            throw new NotImplementedException();
        }
    }
}