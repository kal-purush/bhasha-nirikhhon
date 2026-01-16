using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace FileAlphanumericCounter
{
    public class FileHandler
    {
        private readonly IFilesConfig _config;

        public FileHandler(IFilesConfig config)
        {
            _config = config;
        }

        public string ReadFilteredTextFromFile(string filePath)
        {
            if (File.Exists(filePath))
            {
                if (_config.AllowedExtensions.Contains(Path.GetExtension(filePath)))
                {
                    var fileContent = File.ReadAllText(filePath);
                    var filteredText = new StringBuilder();
                    Regex.Matches(fileContent, _config.RegexPattern)
                        .ToList()
                        .ForEach(e => filteredText.Append(e.Value.ToUpper()));
                    return filteredText.ToString();
                }

                
                throw new ArgumentException($"File has invalid extension! Allowed extensions are: [{string.Join(",", _config.AllowedExtensions)}]!",
                    nameof(filePath));
            }

            throw new ArgumentException("Invalid file path, file doesn't exist!", nameof(filePath));
        }

        public void WriteToFile(string filePath, List<string> lines)
        {
            var invalidCharacters = Path.GetInvalidFileNameChars().ToList();
            invalidCharacters.AddRange(Path.GetInvalidPathChars().ToList());
            if(filePath.IndexOfAny(invalidCharacters.ToArray()) == -1)
            {
                File.WriteAllLines(filePath, lines);
            }
            else
            {
                throw new ArgumentException("Invalid file path!");
            }
        }

    }
}