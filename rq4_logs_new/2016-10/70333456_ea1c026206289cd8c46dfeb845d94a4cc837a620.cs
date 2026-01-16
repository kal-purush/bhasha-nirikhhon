using System;
using System.Collections.Generic;
using System.IO;

namespace RPG
{
    class NameGenerator
    {
        private string prefixFileName;
        private string namesFileName;
        private List<string> names;
        private List<string> prefixes;

        /**
         * DEFAULT CONSTRUCTOR
         * IMPORT: (string) namesFileName, (string) prefixFileName
         */
        public NameGenerator(string namesFileName, string prefixFileName)
        {
            this.prefixFileName = "Resources/" + prefixFileName;
            this.namesFileName = "Resources/" + namesFileName;
            this.names = new List<string>();
            this.prefixes = new List<string>();
        }

        /**
         * FUNCTION: fetchNames
         * IMPORT: None EXPORT: None
         * Used to initialise the random name generator
         */
        public void fetchNames ()
        {
            string line;
            string currentPath = Environment.CurrentDirectory;

            // Read each line from the name file and add it to the list
            try
            {
                StreamReader nameFile = new StreamReader(namesFileName);
                while ((line = nameFile.ReadLine()) != null)
                {
                    names.Add(line);
                }

                // Close file
                nameFile.Close();
            } catch (FileNotFoundException e)
            {
                
            }

            // Read each line from prefix file and add it to the list
            try
            {
                StreamReader prefixFile = new StreamReader(prefixFileName);
                while ((line = prefixFile.ReadLine()) != null)
                {
                    prefixes.Add(line);
                }

                // Close file
                prefixFile.Close();
            }
            catch (FileNotFoundException e)
            {
                
            }
        }

        /**
         * FUNCTION: generateName
         * IMPORT: None EXPORT: None
         * Generates a random name with a prefix and actual name
         */
        public string generateName()
        {
            Random rnd = new Random();

            // Get the amount of elements within the lists
            int namesLength = names.Count;
            int prefixLength = prefixes.Count;

            // Convert list to arrays so we can easily choose a random index from each
            string[] nameArr = names.ToArray();
            string[] prefixArr = prefixes.ToArray();

            // Choose random index from arrays 
            int namesIndex = rnd.Next(0, namesLength - 1);
            int prefixIndex = rnd.Next(0, prefixLength - 1);

            // Return concat string
            return prefixArr[prefixIndex] + " " + nameArr[namesIndex];

        }
    }
}