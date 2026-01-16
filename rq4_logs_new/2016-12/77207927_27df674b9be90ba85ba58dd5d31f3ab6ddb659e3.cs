using System.IO;
using NUnit.Framework;

namespace BulkFtpClient.IntegTest
{
    [TestFixture]
    public class BulkFtpTest
    {
        private readonly string _targetPath;

        public BulkFtpTest()
        {
            _targetPath = DownloadAll();
        }

        [Test]
        public void Assert_File_Downloaded()
        {
            Assert.IsTrue(File.Exists(_targetPath));
        }

        [Test]
        public void Assert_File_Is_Large_Enough()
        {
            var fi = new FileInfo(_targetPath);
            Assert.IsTrue(fi.Length > 1024 * 1024);
        }

        private static string DownloadAll()
        {
            var client = new Ftp.BulkFtpClient();
            var intoPath = Path.GetTempPath();

            client.DownloadDirectory(@"ftp://ftp.arin.net/pub/rr/", intoPath);
            return Path.Combine(intoPath, "arin.db");
        }
    }
}