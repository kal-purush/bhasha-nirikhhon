using Application.Common.Models;
using Application.Interfaces.BlobStorageInterface;
using Application.Interfaces.RepoInterface;
using Domain.Models;
using MediatR;
using Microsoft.Extensions.Logging;

namespace Application.Commands.CVCommands
{
    public class DownloadCVByIdCommandHandler : IRequestHandler<DownloadCVByIdCommand, FileResult>
    {
        private readonly IRepository<CV> _cvRepository;
        private readonly IBlobStorage _blobStorage;
        private readonly ILogger<DownloadCVByIdCommandHandler> _logger;

        public DownloadCVByIdCommandHandler(IRepository<CV> cvRepository, IBlobStorage blobStorage, ILogger<DownloadCVByIdCommandHandler> logger )
        {
            _cvRepository = cvRepository;
            _blobStorage = blobStorage;
            _logger = logger;
        }

        public async Task<FileResult> Handle(DownloadCVByIdCommand request, CancellationToken cancellationToken)
        {
            try
            {
                var cv = await _cvRepository.GetByIdAsync(request.Id, cancellationToken);
                if (cv == null)
                {
                    _logger.LogWarning("CV with ID {CvId} not found.", request.Id);
                    throw new KeyNotFoundException($"CV with ID {request.Id} was not found.");
                }

                var fileBytes = await _blobStorage.DownloadFileAsync(cv.FileUrl);

                return new FileResult
                {
                    FileName = Path.GetFileName(cv.FileUrl),
                    Content = fileBytes
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while downloading CV with ID {CvId}.", request.Id);
                throw;
            }
        }
    }
}