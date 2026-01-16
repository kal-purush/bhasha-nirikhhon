using FileSystemListener.EventArgs;
using FileSystemListener.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileSystemListener
{
    public class FilesWatcher : IWatcher<FileInfo>
    {
        private readonly List<FileSystemWatcher> fileWatchers;
        private readonly ILogger logger;

        public FilesWatcher(IEnumerable<string> directories, ILogger logger)
        {
            this.logger = logger;
            this.fileWatchers = directories.Select(CreateWatcher).ToList();
        }

        public event EventHandler<CreatedEventArgs<FileInfo>> Created;

        private void OnCreated(FileInfo file)
        {
            Created?.Invoke(this, new CreatedEventArgs<FileInfo> { CreatedItem = file });
        }

        private FileSystemWatcher CreateWatcher(string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentNullException("path");
            }

            FileSystemWatcher fileSystemWatcher =
                new FileSystemWatcher(path)
                {
                    NotifyFilter = NotifyFilters.FileName,
                    IncludeSubdirectories = false,
                    EnableRaisingEvents = true
                };

            fileSystemWatcher.Created += (sender, fileSystemEventArgs) =>
            {
                logger.Log(/*string.Format(Strings.FileFoundedTemplate, fileSystemEventArgs.Name)*/$"File found: {fileSystemEventArgs.Name}");
                OnCreated(new FileInfo (fileSystemEventArgs.FullPath));
            };

            return fileSystemWatcher;
        }
    }
}