using Application.Commands.CVCommands;
using Application.Interfaces.BlobStorageInterface;
using Application.Interfaces.RepoInterface;
using Domain.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Internal;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using PdfSharp.Pdf;

namespace TestProject.CvTest
{
    [TestFixture]
    public class CVCommandHandlersTests
    {
        private Mock<IRepository<CV>> _cvRepositoryMock;
        private Mock<IBlobStorage> _blobStorageMock;
        private Mock<ILogger<CreateCVCommandHandler>> _createLoggerMock;
        private Mock<ILogger<DeleteCVByIdCommandHandler>> _deleteLoggerMock;
        private Mock<ILogger<UpdateCVByIdCommandHandler>> _updateLoggerMock;
        private Mock<ILogger<DownloadCVByIdCommandHandler>> _downloadLoggerMock;

        private CreateCVCommandHandler _createHandler;
        private DeleteCVByIdCommandHandler _deleteHandler;
        private UpdateCVByIdCommandHandler _updateHandler;
        private DownloadCVByIdCommandHandler _downloadHandler;

        [SetUp]
        public void Setup()
        {
            _cvRepositoryMock = new Mock<IRepository<CV>>();
            _blobStorageMock = new Mock<IBlobStorage>();
            _createLoggerMock = new Mock<ILogger<CreateCVCommandHandler>>();
            _deleteLoggerMock = new Mock<ILogger<DeleteCVByIdCommandHandler>>();
            _updateLoggerMock = new Mock<ILogger<UpdateCVByIdCommandHandler>>();
            _downloadLoggerMock = new Mock<ILogger<DownloadCVByIdCommandHandler>>();

            _createHandler = new CreateCVCommandHandler(
                _cvRepositoryMock.Object,
                _blobStorageMock.Object,
                _createLoggerMock.Object
            );

            _deleteHandler = new DeleteCVByIdCommandHandler(
                _cvRepositoryMock.Object,
                _blobStorageMock.Object,
                _deleteLoggerMock.Object
            );

            _updateHandler = new UpdateCVByIdCommandHandler(
                _cvRepositoryMock.Object,
                _blobStorageMock.Object,
                _updateLoggerMock.Object
            );

            _downloadHandler = new DownloadCVByIdCommandHandler(
                _cvRepositoryMock.Object,
                _blobStorageMock.Object,
                _downloadLoggerMock.Object
            );
        }

        private IFormFile GenerateValidPdfFile(string fileName)
        {
            var memoryStream = new MemoryStream();

            // Skapa en enkel PDF
            using (var document = new PdfDocument())
            {
                document.Pages.Add(new PdfPage());
                document.Save(memoryStream);
            }

            memoryStream.Position = 0;

            return new FormFile(memoryStream, 0, memoryStream.Length, "file", fileName)
            {
                Headers = new HeaderDictionary(),
                ContentType = "application/pdf"
            };
        }

        [Test]
        public async Task Handle_ValidPDF_CreatesCVSuccessfully()
        {
            // Arrange
            var userId = Guid.NewGuid();
            var fileName = "test.pdf";
            var formFile = GenerateValidPdfFile(fileName);

            var fileUrl = "https://mockstorage.com/test.pdf";
            _blobStorageMock.Setup(x => x.UploadFileAsync(It.IsAny<IFormFile>())).ReturnsAsync(fileUrl);

            var createdCV = new CV { Id = Guid.NewGuid(), FileUrl = fileUrl, FileName = fileName, UploadDate = DateTime.UtcNow, UserId = userId };
            _cvRepositoryMock.Setup(x => x.CreateAsync(It.IsAny<CV>())).ReturnsAsync(createdCV);

            var command = new CreateCVCommand(formFile, userId);

            // Act
            var result = await _createHandler.Handle(command, CancellationToken.None);

            // Assert
            Assert.IsNotNull(result, "Expected a CV object but got null.");
            Assert.AreEqual(fileUrl, result.FileUrl, $"Expected FileUrl to be {fileUrl} but got {result.FileUrl}");
            Assert.AreEqual(fileName, result.FileName, $"Expected FileName to be {fileName} but got {result.FileName}");
            Assert.AreEqual(userId, result.UserId, $"Expected UserId to be {userId} but got {result.UserId}");
        }

        [Test]
        public void Handle_ShouldThrowException_WhenInvalidFileFormat()
        {
            // Arrange
            var fileMock = new Mock<IFormFile>();
            fileMock.Setup(_ => _.FileName).Returns("invalid-file.txt");
            fileMock.Setup(_ => _.ContentType).Returns("text/plain");

            var command = new CreateCVCommand(fileMock.Object, Guid.NewGuid());

            // Act & Assert
            var exception = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await _createHandler.Handle(command, CancellationToken.None));

            Assert.That(exception.Message, Is.EqualTo("Invalid file. Please upload a valid PDF."));
        }

        [Test]
        public async Task Handle_ShouldDeleteCV_WhenValidId()
        {
            // Arrange
            var cvId = Guid.NewGuid();
            var cvEntity = new CV
            {
                Id = cvId,
                FileUrl = "https://fakeblobstorage.com/cv.pdf",
                FileName = "cv.pdf",
                UploadDate = DateTime.UtcNow,
                UserId = Guid.NewGuid()
            };

            _cvRepositoryMock.Setup(repo => repo.GetByIdAsync(cvId, It.IsAny<CancellationToken>())).ReturnsAsync(cvEntity);
            _blobStorageMock.Setup(blob => blob.DeleteFileAsync(It.IsAny<string>())).ReturnsAsync(true);

            // Act
            var command = new DeleteCVByIdCommand(cvId);
            var result = await _deleteHandler.Handle(command, CancellationToken.None);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(cvId, result.Id);

            
            _cvRepositoryMock.Verify(repo => repo.DeleteByIdAsync(cvId), Times.Once);
            _blobStorageMock.Verify(blob => blob.DeleteFileAsync(It.IsAny<string>()), Times.Once);
        }

        [Test]
        public async Task Handle_ShouldThrowException_WhenCVNotFound()
        {
            // Arrange
            var invalidCvId = Guid.NewGuid();

            _cvRepositoryMock.Setup(repo => repo.GetByIdAsync(invalidCvId, It.IsAny<CancellationToken>())).ReturnsAsync((CV)null);

            // Act & Assert
            var command = new DeleteCVByIdCommand(invalidCvId);
            Assert.ThrowsAsync<KeyNotFoundException>(async () => await _deleteHandler.Handle(command, CancellationToken.None));
        }

        [Test]
        public async Task Handle_ShouldUpdateCV_WhenValidIdAndFile()
        {
            // Arrange
            var cvId = Guid.NewGuid();
            var userId = Guid.NewGuid();
            var fileName = "updated-file.pdf";
            var newFile = new Mock<IFormFile>();
            var fileStream = new MemoryStream(); 
            newFile.Setup(f => f.OpenReadStream()).Returns(fileStream);
            newFile.Setup(f => f.FileName).Returns(fileName);

            var cv = new CV
            {
                Id = cvId,
                UserId = userId,
                FileUrl = "https://example.com/old-file.pdf",
                FileName = "old-file.pdf",
                UploadDate = DateTime.UtcNow
            };

            var updatedCv = new CV
            {
                Id = cvId,
                UserId = userId,
                FileUrl = "https://example.com/updated-file.pdf",
                FileName = fileName,
                UploadDate = DateTime.UtcNow,
            };

            _cvRepositoryMock.Setup(r => r.GetByIdAsync(cvId, It.IsAny<CancellationToken>())).ReturnsAsync(cv);
            _cvRepositoryMock.Setup(r => r.UpdateAsync(It.IsAny<CV>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

            _blobStorageMock.Setup(b => b.UploadFileAsync(It.IsAny<IFormFile>())).ReturnsAsync("https://example.com/updated-file.pdf");

            var request = new UpdateCVByIdCommand(cvId, newFile.Object, userId.ToString());

            // Act
            var result = await _updateHandler.Handle(request, CancellationToken.None);

            // Assert
            Assert.NotNull(result);
            Assert.AreEqual("https://example.com/updated-file.pdf", result.FileUrl);
            Assert.AreEqual(fileName, result.FileName);

            _cvRepositoryMock.Verify(r => r.GetByIdAsync(cvId, It.IsAny<CancellationToken>()), Times.Once);
            _cvRepositoryMock.Verify(r => r.UpdateAsync(It.IsAny<CV>(), It.IsAny<CancellationToken>()), Times.Once);
            _blobStorageMock.Verify(b => b.UploadFileAsync(It.IsAny<IFormFile>()), Times.Once);
        }


        [Test]
        public async Task Handle_ShouldDownloadFile_WhenCVExists()
        {
            // Arrange
            var cvId = Guid.NewGuid();
            var fileUrl = "https://example.com/file.pdf";
            var cv = new CV
            {
                Id = cvId,
                FileUrl = fileUrl,
                FileName = "file.pdf",
                UploadDate = DateTime.UtcNow
            };

            var fileStream = new MemoryStream();

            _cvRepositoryMock.Setup(r => r.GetByIdAsync(cvId, It.IsAny<CancellationToken>())).ReturnsAsync(cv);
            _blobStorageMock.Setup(b => b.DownloadFileAsync(fileUrl)).ReturnsAsync(fileStream);

            var request = new DownloadCVByIdCommand(cvId);

            // Act
            var result = await _downloadHandler.Handle(request, CancellationToken.None);

            // Assert
            Assert.NotNull(result);
            Assert.AreEqual(Path.GetFileName(fileUrl), result.FileUrl);
            Assert.AreEqual(fileStream, result.Content);

            _cvRepositoryMock.Verify(r => r.GetByIdAsync(cvId, It.IsAny<CancellationToken>()), Times.Once);
            _blobStorageMock.Verify(b => b.DownloadFileAsync(fileUrl), Times.Once);
        }


        [Test]
        public void Handle_ShouldThrowKeyNotFoundException_WhenCVDoesNotExist()
        {
            // Arrange
            var cvId = Guid.NewGuid();
            _cvRepositoryMock.Setup(r => r.GetByIdAsync(cvId, It.IsAny<CancellationToken>())).ReturnsAsync((CV)null);

            var request = new DownloadCVByIdCommand(cvId);

            // Act & Assert
            var ex = Assert.ThrowsAsync<KeyNotFoundException>(async () => await _downloadHandler.Handle(request, CancellationToken.None));
            Assert.AreEqual($"CV with ID {cvId} was not found.", ex.Message);

            _cvRepositoryMock.Verify(r => r.GetByIdAsync(cvId, It.IsAny<CancellationToken>()), Times.Once);
            _blobStorageMock.Verify(b => b.DownloadFileAsync(It.IsAny<string>()), Times.Never);
        }

        [Test]
        public void Handle_ShouldThrowException_WhenDownloadFails()
        {
            // Arrange
            var cvId = Guid.NewGuid();
            var fileUrl = "https://example.com/file.pdf";
            var cv = new CV
            {
                Id = cvId,
                FileUrl = fileUrl,
                FileName = "file.pdf",
                UploadDate = DateTime.UtcNow
            };

            _cvRepositoryMock.Setup(r => r.GetByIdAsync(cvId, It.IsAny<CancellationToken>())).ReturnsAsync(cv);
            _blobStorageMock.Setup(b => b.DownloadFileAsync(fileUrl)).ThrowsAsync(new Exception("Download failed"));

            var request = new DownloadCVByIdCommand(cvId);

            // Act & Assert
            var ex = Assert.ThrowsAsync<Exception>(async () => await _downloadHandler.Handle(request, CancellationToken.None));
            Assert.AreEqual("Download failed", ex.Message);

            _cvRepositoryMock.Verify(r => r.GetByIdAsync(cvId, It.IsAny<CancellationToken>()), Times.Once);
            _blobStorageMock.Verify(b => b.DownloadFileAsync(fileUrl), Times.Once);
        }

    }
}

