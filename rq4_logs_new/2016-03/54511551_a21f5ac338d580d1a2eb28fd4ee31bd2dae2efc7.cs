using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace AnnkissamTest
{
    class Program
    {
        //main method
        static void Main(string[] args)
        {
            //this files contents are: 
            //this! is a test
            //this is a test
            //and I worked hard
            //hello there
            Console.WriteLine("[" + wordCount("p.txt") + "]");
        }

        //method that takes a file, reads the contents, then takes each word in the file
        //and prints the occurance of each word
        static string wordCount(string p)
        {
            //read in a file of text 
            //first make a new streamreader to read the file named 'p.txt'
            StreamReader readfile = new StreamReader(p);

            //string of text and a list to hold the lines
            string text = "";
            string[] wordlist = new string[0];

            //using a try in case something is wrong
            try
            {
                //a loop to read the contents of the file until there is no more left
                while ((text = readfile.ReadLine()) != null)
                {
                    //split each line of text into individual words and then combine the array with one larger array
                    string[] wordline = text.Split();
                    string[] array1 = wordlist;
                    string[] array2 = wordline;
                    string[] finalarray = new string[array1.Length + array2.Length];
                    Array.Copy(array1, finalarray, array1.Length);
                    Array.Copy(array2, 0, finalarray, array1.Length, array2.Length);
                    wordlist = finalarray;
                }
            }

            //catch an exception if something doesnt go right
            catch (IOException exception)
            {
                //print the error emssage
                Console.WriteLine("Error: " + exception.Message);
            }

            //close the file no matter what
            finally
            {
                //close the file
                readfile.Close();
            }

            //make a copy of the wordlist so that no data is lost when removing items
            List<string> printlist = new List<string>();
            string[] wordlistcopy = wordlist;

            //remove any commas or common characters in each string to get the word alone
            for (int i = 0; i < wordlistcopy.Length; i++)
            {
                wordlistcopy[i] = wordlistcopy[i].Trim(new Char[] {'*', ',', '.', '"', '?', '!', '&'});
            }

            //loop to check if there are any duplicates
            for (int i = 0; i < wordlistcopy.Length; i++)
            {
                if (wordlistcopy[i] != null)
                {
                    int occurance = 1; //increases the occurance to 1 if not null, indicating theres a new word
                    for (int j = 0; j < wordlistcopy.Length; j++)
                    {
                        //if there is a duplicate and it is not the same string
                        if (wordlistcopy[i] == wordlistcopy[j] && i != j)
                        {
                            occurance++; //found a duplicate word, so increasethe occurance
                            wordlistcopy[j] = null; //set that duplicate word to null since you already found it
                        }

                    }
                    //adding the word and its number of occurances to the list to print out later
                    printlist.Add("['" + wordlistcopy[i] + "', " + occurance + "],");
                }

            }
            //turn the array into a string and remove the last ',' at the end 
            string returnstring = String.Join("", printlist);
            returnstring = returnstring.Remove(returnstring.Length - 1);

            //return the string
            return returnstring;
        }
    }
}