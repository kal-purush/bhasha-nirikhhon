using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace FileStats
{
    public class StatsManager
    {
        private static readonly NullReferenceException TransverseNotCalled =
            new NullReferenceException($"Must call {nameof(Transverse)} to populate required field(s)");
        private readonly ILogger<StatsManager> _logger;
        private HashSet<DirectoryInfo> _directories;
        private HashSet<FileInfo> _files;

        private List<FileInfo> _filesSortedBySize;

        public StatsManager(DirectoryInfo rootDirectory, ILogger<StatsManager> logger = null)
        {
            if (!rootDirectory.Exists)
                throw new ArgumentException("Directory doesnt exist", nameof(rootDirectory));

            _logger = logger ?? NullLogger<StatsManager>.Instance;
            RootDirectory = rootDirectory;
        }

        public DirectoryInfo RootDirectory { get; }

        public int DirectoryCount() => Directories.Count;
        
        public int FileCount() => Files.Count;
        
        public IEnumerable<FileInfo> FilesSortedBySizeAscending =>
            _filesSortedBySize ??= Files.OrderBy(file => file.Length).ToList();
        
        private HashSet<FileInfo> Files => _files ?? throw TransverseNotCalled;

        private HashSet<DirectoryInfo> Directories => _directories ?? throw TransverseNotCalled;

        public void Transverse()
        {
            _directories = new HashSet<DirectoryInfo>();
            _files = new HashSet<FileInfo>();
         
            var files = InternalTransverse(RootDirectory);

            foreach (var file in files)
            {
                Files.Add(file);
                _logger.LogTrace("Added file: {FileName}", file.Name);    
            }
        }

        private IEnumerable<FileInfo> InternalTransverse(DirectoryInfo mainDirectory)
        {
            if (Directories == null) throw TransverseNotCalled;
            
            Directories.Add(mainDirectory);
            _logger.LogTrace("Added directory: {DirectoryName}", mainDirectory.FullName);

            var directories = mainDirectory.GetDirectories();
            
            foreach (var directory in directories)
            {
                var files = InternalTransverse(directory);
                
                foreach (var file in files)
                {
                    Files.Add(file);
                    _logger.LogTrace("Added file: {FileName}", file.Name);
                }
            }

            return mainDirectory.GetFiles();
        }

        public long[] DefaultByteRange()
        {
//            const long gibibyte = 1_073_741_824;
            const long gigabyte = 1_000_000_000;
            return ByteRange(gigabyte);
        }       
        
        public long[] ByteRange(long high)
        {
            var range = new List<long>();
            for (long i = 0;; i++)
            {
                if (i == 0)
                {
                    range.Add(0);
                    continue;
                }
                var number = Convert.ToInt64(Math.Pow(2, i));
                range.Add(number);
                if (number >= high) break;
            }
            
            return range.ToArray();
        }
        
        public Dictionary<long, List<FileInfo>> FileSizeDistribution(long[] byteRanges)
        {
            var distribution = new Dictionary<long, List<FileInfo>>();

            for (var i = 0; i < byteRanges.Length; i++)
            {
                var currentRange = new List<FileInfo>();
                foreach (var file in FilesSortedBySizeAscending)
                {
                    // If no previous range
                    if (i == 0)
                    {
                        if (file.Length > byteRanges[i]) continue;
                        currentRange.Add(file);
                        continue;
                    }
                    
                    // If file size is below current range
                    if (file.Length <= byteRanges[i - 1]) continue;
                    
                    // If file size is above current range
                    if (file.Length > byteRanges[i]) continue;
                    
                    currentRange.Add(file);
                }
                distribution.Add(byteRanges[i], currentRange);
            }
            
            return distribution;
        }
        
        public IEnumerable<FileInfo> GetSmallestFiles(long count = 100)
        {
            if (count <= int.MaxValue)
                return FilesSortedBySizeAscending.Take((int) count);
            
            // If count is a long:
            var returnFiles = FilesSortedBySizeAscending;

            while (count > int.MaxValue)
            {
                returnFiles = returnFiles.Skip(int.MaxValue).ToList();
                count -= int.MaxValue;
            }
            returnFiles = returnFiles.Skip(int.MaxValue).ToList();
            
            return returnFiles;
        }
    }
}