using SimpleSmtpInterceptor.Data.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;

namespace SimpleSmtpInterceptor.Lib
{
    public class AttachmentCompressor
    {
        private readonly IList<EmailAttachment> _attachments;

        public AttachmentCompressor(IList<EmailAttachment> attachments)
        {
            _attachments = attachments;
        }

        public RawFile SaveAsZipArchive(string fileName)
        {
            var lstCleanUp = new List<string>(_attachments.Count + 1);

            //If the filename does not include the zip extension add it
            if (!fileName.EndsWith(".zip")) fileName += ".zip";

            var obj = new RawFile
            {
                FileName = fileName
            };

            //Get a temporary path to work with
            var path = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

            //Get a separate directory so that the zip file can be created to avoid IO access exceptions
            var pathZip = path + "zip";

            //Create base working directory
            Directory.CreateDirectory(path);

            //Create directory for zip file
            Directory.CreateDirectory(pathZip);

            //Save to / Save as zip file
            var zipFilePath = Path.Combine(pathZip, fileName);

            lstCleanUp.Add(zipFilePath);

            //For each attachment
            foreach (var a in _attachments)
            {
                //Decode the base 64 string into a byte array
                var bytes = Base64Decode(a.AttachmentBase64);

                //Save as
                var filePath = Path.Combine(path, a.Name);

                //Write byte array to temp path with original name
                File.WriteAllBytes(filePath, bytes);

                lstCleanUp.Add(filePath);
            }

            //Archive everything into a zip file in the temp path
            ZipFile.CreateFromDirectory(path, zipFilePath);

            //Create a byte array from the zip file
            obj.Contents = File.ReadAllBytes(zipFilePath);

            //Delete all files created
            lstCleanUp.ForEach(File.Delete);

            //Delete the working directories created
            Directory.Delete(path);

            Directory.Delete(pathZip);

            return obj;
        }

        private static byte[] Base64Decode(string base64EncodedString)
        {
            var data = Convert.FromBase64String(base64EncodedString);

            return data;
        }
    }
}