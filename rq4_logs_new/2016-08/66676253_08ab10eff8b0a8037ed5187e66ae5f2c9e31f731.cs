using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DeveloperTestApplication
{
    public class DirectoryMapper
    {
        public List<DirectoryInfo> RecurseDirectories(DirectoryInfo rootDirectoryInfo)
        {
            List<DirectoryInfo> listOfFoundDirectories = new List<DirectoryInfo>();

            List<DirectoryInfo> listOfFirstTierDirectories = rootDirectoryInfo.GetDirectories().ToList();

            foreach (DirectoryInfo directory in listOfFirstTierDirectories)
            {
                listOfFoundDirectories.Add(directory);
                RecurseDirectories(directory);
            }

            return listOfFoundDirectories;
        }

        public bool WriteFileNamesToDiskInEncryptedSettingsFile(string fileName)
        {
            return false;
        }

        public List<FileInfo> GetListOfReadOnlyFiles()
        {
            return null;
        }

        public bool RemoveReadOnlyFromFile(string fileToResetPermissionsOn)
        {
            return false;
        }

        public List<DirectoryInfo> SortDirectoriesByName(List<DirectoryInfo> listOfDirectoriesToSort)
        {
            return null;
        }
    }
}