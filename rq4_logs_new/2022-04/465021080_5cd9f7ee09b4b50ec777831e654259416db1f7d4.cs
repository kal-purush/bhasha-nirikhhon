using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace GraduateProject.Common.Services.FileManager
{

    public class FileManagerService : IFileManagerService
    {
        private readonly IWebHostEnvironment _environment;

        public FileManagerService(IWebHostEnvironment environment)
        {
            _environment = environment;
        }

        //---------------------------------------------------------------------------------------------------
        /// <summary>
        /// 
        /// </summary>
        /// <param name="file">File You want yo save</param>
        /// <param name="folder">sub folder if any</param>
        /// <param name="generateRandomFileName">to generate random file name</param>
        /// <param name="mediaType">what ever</param>
        /// <returns></returns>
        public async Task<string> UploadFileAsync(IFormFile file,
            string referenceType,
            bool generateRandomFileName = true)
        {

            try
            {
                if (file == null)
                {
                    return null;
                }

                // file name
                string fileName = file.FileName;
                if (generateRandomFileName)
                    fileName = Guid.NewGuid().ToString() + Path.GetExtension(file.FileName);

                var pathToSave = Path.Combine(
                           _environment.WebRootPath,
                           referenceType);

                if (!Directory.Exists(pathToSave))
                    Directory.CreateDirectory(pathToSave);

                var fullFilePath = Path.Combine(pathToSave, fileName);
                using var stream = File.Create(fullFilePath);
                {
                    await file.CopyToAsync(stream);
                }
                return string.Format("/{0}/{1}",
                                referenceType.ToString(),
                                fileName);
            }
            catch (Exception)
            {
                //TODO log
                return null;
            }
        }

        //---------------------------------------------------------------------------------------------------
        public async Task DeleteFileAsync(string url)
        {
            try
            {

                if (url != null)
                {
                    url = url.Trim().Replace("/", "\\");
                    var filepath = _environment.WebRootPath + url; //Path.Combine(_environment.WebRootPath,"s\\ssss");
                    await Task.Run(() =>
                    {
                        if (File.Exists(filepath))
                            File.Delete(filepath);

                    });
                }
             
            }
            catch (Exception)
            {
                //TODO log
            }
        }
        //---------------------------------------------------------------------------------------------------
        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }

    }
}