using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.FileProviders;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ComicBoxApi.App.FileBrowser
{
    public interface IFilePathFinder
    {
        void AppendPathContext(params string[] subpaths);
        IReadOnlyCollection<IFileInfo> GetDirectoryContents(ListMode listMode);
        string GetNextFileNameOrDefault(string file, string matchExtension);
        FilePath GetPath();
        IFileInfo GetThumbnailFileInfoForFile(string file);
        FilePath LocateFile(string file);
        FilePath LocateFirstFile(string matchExtension);
        void SetPathContext(params string[] subpaths);
    }

    public class FilePathFinder : IFilePathFinder
    {
        private readonly List<string> _subpaths;

        private readonly IFileProvider _fileProvider;

        private readonly IConfiguration _configuration;

        private readonly string _rootPath;

        public FilePathFinder(IFileProvider fileProvider, IConfiguration configuration)
        {
            _subpaths = new List<string>();
            _fileProvider = fileProvider;
            _configuration = configuration;
            _rootPath = Path.GetDirectoryName(fileProvider.GetDirectoryContents("").First().PhysicalPath);
        }

        private FilePathFinder BuildNewInstance()
        {
            return new FilePathFinder(_fileProvider, _configuration);
        }

        public void SetPathContext(params string[] subpaths)
        {
            _subpaths.Clear();
            AppendPathContext(subpaths);
        }

        public void AppendPathContext(params string[] subpaths)
        {
            _subpaths.AddRange(subpaths);
        }

        public FilePath GetPath()
        {
            return new FilePath(_rootPath, _subpaths.ToArray());
        }

        public IReadOnlyCollection<IFileInfo> GetDirectoryContents(ListMode listMode)
        {
            IEnumerable<IFileInfo> directoryContents = _fileProvider.GetDirectoryContents(GetPath().RelativePath);

            switch(listMode)
            {
                case ListMode.OnlyDirectories:
                    directoryContents = directoryContents.Where(f => f.IsDirectory);
                    break;
                case ListMode.OnlyFiles:
                    directoryContents = directoryContents.Where(f => !f.IsDirectory);
                    break;
                default:
                    break;
            }

            return directoryContents.ToArray();
        }

        public IFileInfo GetThumbnailFileInfoForFile(string file)
        {
            return _fileProvider.GetFileInfo(LocateFile(file).RelativePath);
        }

        public FilePath LocateFile(string file)
        {
            return new FilePath(_rootPath, GetPath().RelativePath, file);
        }

        public FilePath LocateFirstFile(string matchExtension)
        {
            FilePathFinder filePathFinder = BuildNewInstance();
            filePathFinder.SetPathContext(_subpaths.ToArray());

            return LocateFirstFile(filePathFinder, string.Empty, matchExtension);
        }

        private FilePath LocateFirstFile(FilePathFinder pathFinder, string appendPath, string matchExtension)
        {
            pathFinder.AppendPathContext(appendPath);
            var currentDir = pathFinder.GetDirectoryContents(ListMode.All);
            var firstFile = currentDir.FirstOrDefault(f => matchExtension.Equals(Path.GetExtension(f.Name)));
            if (firstFile != null)
            {
                return new FilePath(_rootPath, GetPath().RelativePath, firstFile.Name);
            }
            else
            {
                return LocateFirstFile(pathFinder, currentDir.First(f => f.IsDirectory).Name, matchExtension);
            }
        }

        public string GetNextFileNameOrDefault(string file, string matchExtension)
        {
            FilePathFinder filePathFinder = BuildNewInstance();
            filePathFinder.SetPathContext(_subpaths.TakeWhile(subpath => !subpath.Equals(file)).ToArray());
            var currentDir = filePathFinder.GetDirectoryContents(ListMode.OnlyFiles).Where(f => matchExtension.Equals(Path.GetExtension(f.Name)));
            var nextFile = currentDir.SkipWhile(f => !f.Name.Equals(file)).Skip(1).FirstOrDefault();
            return nextFile != null ? nextFile.Name : null;
        }
    }
}