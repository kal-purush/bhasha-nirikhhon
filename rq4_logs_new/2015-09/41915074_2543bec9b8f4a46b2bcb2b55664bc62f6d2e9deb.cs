using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace OrderSystem
{
    class CustomersFunctions
    {
        private string FilePath = @"C:\Users\Giannis\Documents\GitHub\OrderSystem\Data";
        private void CheckFileExist(string FilePath)
        {

        }

        public void CreateFileData(string ID)
        {
            string FileName = ID + ".txt";
            string FullPath = System.IO.Path.Combine(FilePath, FileName);
            File.Create(FullPath);
        }

        public void Add(string ID, string FirstName, string LastName, string Telephone, string Address)
        {
            string FileName = ID + ".txt";
            string FullPath = System.IO.Path.Combine(FilePath, FileName);
            string[] lines = { FirstName, LastName, Telephone, Address };

            System.IO.Directory.CreateDirectory(FilePath);
            try
            {

                if (!System.IO.File.Exists(FullPath))
                {
                    System.IO.File.WriteAllLines(FullPath, lines);
                }
                else
                {
                    Console.WriteLine("File \"{0}\" already exists.", FileName);
                    return;
                }
            }
            catch (System.UnauthorizedAccessException e)
            {
                Console.WriteLine(e.Message);
            }
        }

        public void Delete(int ID)
        {
            File.Delete(FilePath + ID + ".txt");
        }

        public void Update(int ID, string FirstName, string LastName, string Telephone, string Address)
        {

        }

    }
}