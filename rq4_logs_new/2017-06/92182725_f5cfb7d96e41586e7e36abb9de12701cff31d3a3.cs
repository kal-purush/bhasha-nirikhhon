using ComicBoxApi.App.FileBrowser;
using ComicBoxApi.App.PdfReader;
using Microsoft.Extensions.FileProviders;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ComicBoxApi.App.Imaging
{

    public class ThumbnailWorker
    {
        private readonly IFilePathFinder _filePathFinder;

        private readonly IImageService _imageService;

        private Task _worker;

        private Dictionary<IFileInfo, IFileInfo> _inProgress;

        private List<ThumbnailWorkerError> _errors;

        public ThumbnailWorker(IFilePathFinder filePathFinder, IImageService imageService)
        {
            _filePathFinder = filePathFinder;
            _imageService = imageService;

            _inProgress = new Dictionary<IFileInfo, IFileInfo>();
            _errors = new List<ThumbnailWorkerError>();
        }

        public Task GetTask()
        {
            if (_worker == null)
            {
                _worker = Task.Factory.StartNew(() => BrowseAndGenerate());
            }

            return _worker;
        }

        public void BrowseAndGenerate()
        {
            BrowseAndGenerate("");

            while (_inProgress.Any(kvp => kvp.Value != null))
            {
                var current = _inProgress.First(kvp => kvp.Value != null);
                ProcessFile(current.Key, current.Value);
                _inProgress[current.Key] = null;
            }
        }

        public bool IsInProgress
        {
            get
            {
                return GetTask().Status < TaskStatus.RanToCompletion;
            }
        }

        public string InProgressMessage
        {
            get
            {
                return string.Format("Building thumbnails is in progress... {0}/{1}", _inProgress.Count(kvp => kvp.Value == null), _inProgress.Count());
            }
        }

        public bool IsFaulted
        {
            get
            {
                return GetTask().IsFaulted || _errors.Any();
            }
        }

        public string DisplayErrors
        {
            get
            {
                return string.Join(" - ", _errors.Select(error => error.ToString()));
            }
        }

        private void BrowseAndGenerate(params string[] subpaths)
        {
            _filePathFinder.SetPathContext(subpaths);
            var dirs = _filePathFinder.GetDirectoryContents(ListMode.All);
            foreach (var dir in dirs)
            {
                if (dir.IsDirectory)
                {
                    BrowseAndGenerate(subpaths.Union(new[] { dir.Name }).ToArray());
                }
                else if (BookInfoService.DefaultFileContainerExtension.Equals(Path.GetExtension(dir.Name)))
                {
                    var fileInfo = new ThumbnailProvider(_filePathFinder).GetThumbnail(dir.Name);
                    if (!fileInfo.Exists)
                    {
                        _inProgress.Add(dir, fileInfo);
                    }
                }
            }
        }

        private void ProcessFile(IFileInfo currentFile, IFileInfo currentFileThumbnail)
        {
            try
            {
                var fileContent = new PdfReaderService(currentFile.PhysicalPath).ReadImageFirstPage();
                var thumbnailContent = _imageService.ScaleAsThumbnail(fileContent);

                var fileInfoPhysicalPath = string.Format("{0}.jpg", currentFile.PhysicalPath);
                using (StreamWriter sw = new StreamWriter(fileInfoPhysicalPath))
                {
                    sw.BaseStream.Write(thumbnailContent, 0, thumbnailContent.Length);
                }
            }
            catch (Exception ex)
            {
                _errors.Add(new ThumbnailWorkerError(currentFileThumbnail.Name, ex));
            }
        }
    }
}