using System;
using System.Collections.Generic;
using System.Linq;
using Ctlg.Core;
using Ctlg.Core.Interfaces;
using File = Ctlg.Core.File;

namespace Ctlg.Service
{
    public class TextFileSnapshotFactory : ISnapshotFactory
    {
        public TextFileSnapshotFactory(IFilesystemService filesystemService, IDataService dataService)
        {
            FilesystemService = filesystemService;
            DataService = dataService;
        }

        public ISnapshot GetSnapshot(Config config, string name, string timestamp)
        {
            var snapshotDirectory = GetSnapshotDirectory(config.Path, name);
            FilesystemService.CreateDirectory(snapshotDirectory);

            var snapshotFileName = timestamp ?? GenerateSnapshotFileName();
            var fullPath = FilesystemService.CombinePath(snapshotDirectory, snapshotFileName);
            var hashAlgorithm = DataService.GetHashAlgorithm(config.HashAlgorithmName);

            return new TextFileSnapshot(FilesystemService, hashAlgorithm, fullPath, name, snapshotFileName);
        }

        public List<string> GetTimestamps(Config config, string name)
        {
            var snapshotDirectory = GetSnapshotDirectory(config.Path, name);
            return GetSnapshotFiles(snapshotDirectory).Select(d => d.Name).OrderBy(s => s).ToList();
        }

        private string GetSnapshotDirectory(string root, string snapshotName)
        {
            return FilesystemService.CombinePath(root, "snapshots", snapshotName);
        }

        private IEnumerable<File> GetSnapshotFiles(string snapshotDirectory)
        {
            if (!FilesystemService.DirectoryExists(snapshotDirectory))
            {
                return Enumerable.Empty<File>();
            }
            return FilesystemService.EnumerateFiles(snapshotDirectory, "????-??-??_??-??-??");
        }

        private string GenerateSnapshotFileName()
        {
            return FormatSnapshotName(DateTime.UtcNow);
        }

        private string FormatSnapshotName(DateTime date)
        {
            return date.ToString("yyyy-MM-dd_HH-mm-ss");
        }

        private IFilesystemService FilesystemService { get; }
        private IDataService DataService { get; }
    }
}