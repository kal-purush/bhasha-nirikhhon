using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using SafeCity.FileStorage.Impl;

namespace SafeCity.FileStorage.Tests
{
    public class FileStorageServiceTests
    {
        private readonly string _pathToFiles = Path.Combine(Directory.GetCurrentDirectory(), "Files");
        
        [Test]
        public async Task SaveAttachmentTest()
        {
            var originalImageBytes = await File.ReadAllBytesAsync(Path.Combine(_pathToFiles, "1.jpg"));
            var imageHandler = new ImageHandler();
            var fsService = new FileStorageService(imageHandler);
            var resultFile = await fsService.SaveAttachment(null);
            var savedFile = await File.ReadAllBytesAsync(resultFile);
            
            Assert.IsTrue(savedFile != null);
            Assert.IsTrue(savedFile.Length > 1);
        }
    }
}