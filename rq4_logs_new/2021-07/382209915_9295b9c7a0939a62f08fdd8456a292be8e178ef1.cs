using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Address_Book
{
    class FileOperations
    {
        const string filepath = @"D:\Random Programs\Address_Book\Address_Book\DictFile.txt";

        //Write content to file
        public static void GetDictionary(Dictionary<string, List<AddressBookSystem>> addressbooknames)
        {
            //Reset the file as empty
            File.WriteAllText(filepath, string.Empty);
            //Iterate over each AddressBook in Dictionary
            foreach (KeyValuePair<string, List<AddressBookSystem>> kvp in addressbooknames)
            {
                //Append key in file
                File.AppendAllText(filepath, "Address Book: " + kvp.Key + Environment.NewLine);
                foreach (var member in kvp.Value)
                {
                    //Append Values or Contacts in file
                    File.AppendAllText(filepath, member.ToString() + Environment.NewLine);
                }
                //Enter a new line to File
                File.AppendAllText(filepath, Environment.NewLine);
                Console.WriteLine("----------The Content written in the file----------");
                //Read content present in File
                ReadAddressBook();
            }

        }
        public static void ReadAddressBook()
        {
            try
            {
                //Read All content of File
                string[] text = File.ReadAllLines(filepath);
                foreach (var mem in text)
                    Console.WriteLine(mem);
                Console.WriteLine("\n");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}