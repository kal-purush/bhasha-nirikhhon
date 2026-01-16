using System;
using System.Collections.Generic;

namespace PhoneBook
{
    class ConsoleDateBuilder
    {
        /// <summary>
        /// Load start info in info block
        /// </summary>
        /// <param name="phoneBook"></param>
        public static void LoadStartInfo(Contacts phoneBook,ref int contactStartIndex,
            ref int contactLastIndex )
        {
            List<string> myContactsList = new List<string>();
            int startIndex = contactStartIndex;
            contactLastIndex=startIndex+20;

            if(phoneBook.GetLenght<(startIndex+20)){
                contactLastIndex=startIndex+(phoneBook.GetLenght-startIndex);
            }

            myContactsList = phoneBook.GetListOfContacts();

            // Output index
            for (int i = startIndex; i < contactLastIndex; i++)
            {
                Console.SetCursorPosition(2, 5 + i);
                Console.Write(string.Format("{0}", i + 1).PadLeft(3, ' '));
            }

            // Output Names
            for (int i = startIndex; i < contactLastIndex; i++)
            {
                string[] pairNamePhone = myContactsList[i].Split('/');
                string[] nameTemp = pairNamePhone[0].Split(' ');

                Console.SetCursorPosition(8, 5 + i);

                if (pairNamePhone[0].Length > 45){
                    Console.Write(string.Format("{0} {1}.{2}.", nameTemp[0],
                        nameTemp[1][0], nameTemp[2][0]));
                }
                else {
                    Console.Write(pairNamePhone[0]);
                }

                
            }

            // Output PhoneNumbers
            for (int i = startIndex; i < contactLastIndex; i++)
            {
                string[] pairNamePhone = myContactsList[i].Split('/');
                string[] nameTemp = pairNamePhone[0].Split(' ');

                Console.SetCursorPosition(50, 5 + i);
                Console.Write(string.Format("{0} ", pairNamePhone[1]));
            }

        }

        /// <summary>
        /// Load command information in command frame
        /// </summary>
        public static void LoadCommandInfo(Contacts myPhoneBook)
        {
            // Command block
            Console.SetCursorPosition(81, 2);
            Console.Write("Commands");

            for (int i = 0; i < 5; i++) {
                Console.SetCursorPosition(74, 4+i);
                Console.Write((Commands)i);
            }

            // Status block
            Console.SetCursorPosition(82, 16);
            Console.Write("Status");
            Console.SetCursorPosition(74, 18);
            Console.Write("Current lenght: {0}", myPhoneBook.GetLenght);
        }

        
    }
}