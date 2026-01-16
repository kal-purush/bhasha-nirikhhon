using FileAPI.MFT.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using FileInfo = Ftaas.Sdk.Base.FileInfo;

namespace FileAPI.MFT.FileSystem.NetCore22.Examples
{
    public class Download : Startup
    {
        private static ITestOutputHelper _output;

        public Download(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task DownloadOneFile()
        {
            #region Custom parameters

            var tenantId = "MyTenantId"; // Only necessary for multi-tenant token.

            #endregion

            _output.WriteTittle("Executing FileSystem.SDK example: Download one file");

            // First you need a valid file ID so you can download it.
            // If you already know the ID of an uploaded file, you can use, instead, that ID.
            var fileId = GetRandomUploadedFile(tenantId).FileId.ToString();

            var downloadPath = Path.Combine(FilesBaseDirectory, "downloadedFile.txt");

            //optional token source to cancel operations after some milliseconds:
            var tokenSource = new CancellationTokenSource();
            tokenSource.CancelAfter(30000);

            await FileSystem.DownloadFileAsync(fileId, downloadPath, tenantId: tenantId, tokenSource.Token);

            Assert.True(File.Exists(downloadPath), $"File was not downloaded correctly to {downloadPath}");

            _output.WriteLine($"File downloaded at {downloadPath}");
        }

        [Fact]
        public async Task DownloadTwoFilesInParallel()
        {
            // As the call is asynchronous, it is possible to do several calls in parallel.

            #region Custom parameters

            var tenantId = "MyTenantId"; // Only necessary for multi-tenant token.

            #endregion

            _output.WriteTittle("Executing FileSystem.SDK example: Download two files in parallel");

            // First you need a valid file ID so you can download it.
            // If you already know the ID of an uploaded file, you can use, instead, that ID.
            var fileIdFirst = GetRandomUploadedFile(tenantId).FileId.ToString();
            var fileIdSecond = GetRandomUploadedFile(tenantId).FileId.ToString();

            var downloadPathFirst = Path.Combine(FilesBaseDirectory, "downloadedInParallelFile_First.txt");
            var downloadPathSecond = Path.Combine(FilesBaseDirectory, "downloadedInParallelFile_Second.txt");

            var downloadTasks = new List<Task>
            {
                FileSystem.DownloadFileAsync(fileIdFirst, downloadPathFirst, tenantId: tenantId),
                FileSystem.DownloadFileAsync(fileIdSecond, downloadPathSecond, tenantId: tenantId)
            };

            // Wait for the files to be downloaded and print the results.
            // If you only care about all files being downloaded and not the order, you can use Task.WhenAll instead.
            var firstDownloadedFile = await Task.WhenAny(downloadTasks);

            downloadTasks.Remove(firstDownloadedFile);
            Assert.True(File.Exists(downloadPathFirst) || File.Exists(downloadPathSecond),
                $"File was not downloaded correctly to {FilesBaseDirectory}");
            _output.WriteLine($"First downloaded file: {downloadPathFirst}");

            await Task.WhenAny(downloadTasks);

            Assert.True(File.Exists(downloadPathFirst) && File.Exists(downloadPathSecond),
                $"Files were not downloaded correctly to {FilesBaseDirectory}");
            _output.WriteLine($"Second downloaded file: {downloadPathSecond}");
        }

        #region Helper methods

        private static readonly Random _random = new Random();

        private FileInfo GetRandomUploadedFile(string tenantId)
        {
            var filter = "Status eq 'All'";
            var randomUploadedFile = FileSystem.GetAvailableFilesAsync(filter: filter, tenantId: tenantId).Result.Data;

            if (!randomUploadedFile.Any())
                throw new ArgumentOutOfRangeException(
                    $"No uploaded file for tenantId <{tenantId}>. Please, execute the UploadExamples tests before theses.");

            return randomUploadedFile
                .ElementAt(_random.Next(randomUploadedFile.Count()));
        }

        #endregion
    }
}