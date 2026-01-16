using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.File;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Threading.Tasks;
using TestApp.Core.FileStorage.Interfaces;
using TestApp.Core.Models.Analysis;

namespace TestApp.Core.FileStorage.Repositories
{
    public class AzureFileShareRepository : IDataRepository
    {
        private readonly CloudFileDirectory _uploads;
        private readonly CloudFileDirectory _reports;

        public AzureFileShareRepository(
            string connectionString,
            string root,
            string uploads,
            string reports)
        {
            var rootDirectory = CloudStorageAccount.Parse(connectionString)
                .CreateCloudFileClient()
                .GetShareReference(root)
                .GetRootDirectoryReference();

            this._uploads = rootDirectory.GetDirectoryReference(uploads);
            this._reports = rootDirectory.GetDirectoryReference(reports);
        }

        #region IDataRepository Members

        public Task<AnalysisResult> GetAnalysisResult(string name)
        {
            var result = new AnalysisResult();

            var cloudFile = this._reports.GetFileReference(name);

            if (cloudFile.Exists())
            {
                using (var reader = new StreamReader(cloudFile.OpenRead()))
                {
                    var readTask = reader.ReadToEndAsync();
                    Task.WaitAll(readTask);

                    result = JsonConvert.DeserializeObject<AnalysisResult>(readTask.Result);
                }
            }

            return Task.FromResult(result);
        }

        public Task<string> GetRawData(string name)
        {
            var cloudFile = this._uploads.GetFileReference(name);

            using (var reader = new StreamReader(cloudFile.OpenRead()))
            {
                return Task.FromResult(reader.ReadToEnd());
            }
        }

        public Task<string> SaveAnalysisResult(AnalysisResult result, string name)
        {
            var fileName = $"analysis_{name}_{Guid.NewGuid()}.json";

            Task.WaitAll(this.SaveResult(result, fileName));

            return Task.FromResult(fileName);
        }

        public Task<string> SaveComparisonResult(AnalysisResult result, string name)
        {
            var fileName = $"comparison_{name}_{Guid.NewGuid()}.json";

            Task.WaitAll(this.SaveResult(result, fileName));

            return Task.FromResult(fileName);
        }

        public bool SaveFile(string name, Stream stream)
        {
            var cloudFile = this._uploads.GetFileReference($"upload_{Guid.NewGuid()}_{name}");

            using (stream)
            {
                Task.WaitAll(cloudFile.UploadFromStreamAsync(stream));
            }

            return true;
        }
        
        #endregion

        #region Private Members

        private Task SaveResult(AnalysisResult result, string fileName)
        {
            var content = JsonConvert.SerializeObject(result);

            var cloudFile = this._reports.GetFileReference(fileName);

            return cloudFile.UploadTextAsync(content);
        }
        
        #endregion
    }
}