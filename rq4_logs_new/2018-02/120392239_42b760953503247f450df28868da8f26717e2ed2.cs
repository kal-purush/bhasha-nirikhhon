using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Subrosa
{
    public class StorageServices
    {
        private StreamWriter _writer { get; set; }

        public void SetupWriter(string location, string fileName)
        {
            if (String.IsNullOrWhiteSpace(location))
                location = @"C:\";

            if (String.IsNullOrWhiteSpace(fileName))
                fileName = "log.txt";

            this._writer = new StreamWriter(location + fileName);
            this._writer.WriteLine("Setup Complete" + "\n");
        }

    }
}