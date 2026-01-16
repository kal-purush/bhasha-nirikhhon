using System.IO;
using System.Windows.Forms;
using System;

namespace Fresh
{
    class Archives
    {
        public static bool ValidXLSFile(ref string filePath)
        {
            string appPath = Path.GetDirectoryName(Application.ExecutablePath);

            bool validFile = false;
            if (filePath != null)
            {
                if (File.Exists(filePath))
                {
                    validFile = true;
                }
                else
                {
                    filePath = appPath + Path.DirectorySeparatorChar + filePath;
                    if (File.Exists(filePath))
                    {
                        validFile = true;
                    }
                }
            }
            if (validFile)
            {
                validFile = false;
                filePath = Path.GetFullPath(filePath);

                //this is to test for invalid file types
                if (Path.GetExtension(filePath).ToLower() != ".xls")
                    validFile = true;
                if (Path.GetExtension(filePath).ToLower() != ".xlsx")
                    validFile = true;
                if (Path.GetExtension(filePath).ToLower() != ".csv")
                    validFile = true;
            }
            return validFile;
        }
    }
}