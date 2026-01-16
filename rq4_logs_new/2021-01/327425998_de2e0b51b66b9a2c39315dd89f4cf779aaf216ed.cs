using System;
using System.IO;
using System.Text.RegularExpressions;

namespace ParseHyperVReport
{
    internal class Program
    {
        /// <summary>
        /// This application parses the output from Get-HyperVInventory 2.4 PowerShell script.
        /// The script can be downloaded from here: https://gallery.technet.microsoft.com/Get-HyperVInventory-Create-2c368c50
        /// This application allows to extract specific information enclosed between "header" marker specified by the record-pattern
        /// argument. 
        /// </summary>
        /// <param name="source">Specifies the filename of the hyperV report file</param>
        /// <param name="recordPattern">String specifying how a given text line should begin to be considered the marker of a new record</param>
        /// <param name="attributes">An array of strings containing the list of attributes to extract from the report file. Each entry can be a simple string matching the text before the colon (:) separating the value or a name=regex specifying the name of the attribute and a regex specifying how to extra the value in from the right side of the colon marker. If using a regex it must contain at last one capturing group matching the value attempting to extract</param>

        // ReSharper disable once UnusedMember.Local
        private static void Main(string source, string recordPattern, string[] attributes)
        {
            if (!File.Exists(source))
                throw new FileNotFoundException($"File {source} doesn't exist");
            if (recordPattern == "")
                throw new ArgumentException("record-pattern can't be a null string");
            if (attributes == null || attributes.Length <= 0)
                throw new ArgumentException("attributes must contain at least one attribute");

            var reportContents = File.ReadAllText(source);
            var lines = reportContents.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
            var withinRecordSection = false;
            var record = new string[attributes.Length];

            Console.WriteLine(string.Join(',', attributes));
            foreach (var line in lines)
            {
                if (!withinRecordSection)
                {
                    withinRecordSection = line.StartsWith(recordPattern);
                    if (!withinRecordSection)
                        continue;
                }
                else if (line.StartsWith(recordPattern))
                {
                    Console.WriteLine(string.Join(',', record));
                    for (var i = 0; i < record.Length; i++)
                        record[i] = "";
                }

                var kvp = line.Split(':');
                if (kvp.Length != 2)
                    continue; // Unsupported key-value pair format

                var index = 0;
                var containsRegEx = false;
                foreach (var attribute in attributes)
                {
                    containsRegEx = attribute.Contains("=");
                    if (attribute.Substring(0, containsRegEx ? attribute.IndexOf('=') : attribute.Length) == kvp[0])
                        break;
                    index++;
                }

                if (index >= attributes.Length)
                    continue; // Attribute not found in list passed in command line

                if (!containsRegEx)
                {
                    record[index] = kvp[1].Trim();
                    continue;
                }

                var extractionRegEx = attributes[index].Substring(kvp[0].Length + 1);
                if (extractionRegEx == "")
                    throw new ArgumentException("Extraction regEx can't be a null string");
                var matches = Regex.Match(kvp[1], extractionRegEx, RegexOptions.Compiled);
                if (!matches.Success)
                    continue;
                record[index] = matches.Groups[1].Value.Trim();
            }
        }
    }
}