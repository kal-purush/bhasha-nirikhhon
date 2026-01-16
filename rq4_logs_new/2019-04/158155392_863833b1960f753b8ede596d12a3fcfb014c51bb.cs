using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Http;
using System.Web.Hosting;
using BroadwayBuilder.Api.Models;

namespace BroadwayBuilder.Api
{
    public class FileValidator
    {
        public ValidationResultModel ValidateFiles(HttpFileCollection fileCollection, List<string> validExtensions, int fileSize, int fileAmount)
        {
            var result = new ValidationResultModel()
            {
                ValidationSuccessful = false,
                Reasons = new HashSet<string>()
            };
                      
            // Max file size
            int MaxContentLength = fileSize;

            var fileCount = fileCollection.Count;

            if (fileCount < 1)
            {
                result.Reasons.Add("Please upload a file!");
            }
            else if (fileCount > fileAmount)
            {
                result.Reasons.Add($"Too many files! You may only upload {fileAmount} files");
            }

            foreach (string filename in fileCollection)
            {
                // Grab current file of the request
                var putFile = fileCollection[filename];


                // Continue if the file has content
                if (putFile != null && putFile.ContentLength > 0)
                {
                    // Checks the current extension of the current file
                    var ext = putFile.FileName.Substring(putFile.FileName.LastIndexOf('.'));
                    var extension = ext.ToLower();

                    // File extension is not valid
                    if (!validExtensions.Contains(extension))
                    {
                        // Todo: Log the error that occurs
                        result.Reasons.Add($"File must match file types: {string.Join(", ", validExtensions)}");
                    }
                    // File size is too big
                    else if (putFile.ContentLength > MaxContentLength)
                    {
                        //var message = string.Format("Please Upload a file upto 1 mb.");
                        // Todo: log the error that occurs
                        result.Reasons.Add($"File exceeds max limit of {fileSize / 1024 / 1024} MB");
                    }
                }
            }

            if (!result.Reasons.Any())
            {
                result.ValidationSuccessful = true;
            }

            return result;
        }
    }
}