using System;
using System.Collections.Generic;
using System.IO;

namespace FileIndexer.Controller
{
    public class IndexerController
    {
        private List<FileInfo> _dataInfo;
        private string _filePath;

        public List<FileInfo> DataInfo
        {
            get
            {
                return _dataInfo;
            }

            set
            {
                _dataInfo = value;
            }
        }
        public string FilePath
        {
            get
            {
                return _filePath;
            }

            set
            {
                _filePath = value;
            }
        }

        public TreeNode<FileSystemInfo> GetAllFilesRecursively(string filepath)
        {
            if (!Directory.Exists(filepath))
                return null;

            DirectoryInfo rootDirectory = new DirectoryInfo(filepath);
            TreeNode<FileSystemInfo> myTreeResult = new TreeNode<FileSystemInfo>(rootDirectory);
            List<FileSystemInfo> directories = new List<FileSystemInfo>();

            myTreeResult.AddChildren(rootDirectory.GetFiles());
            directories.AddRange(rootDirectory.GetDirectories());

            foreach (DirectoryInfo directory in directories)
            {
                TreeNode<FileSystemInfo> subTree = GetAllFilesRecursively(directory.FullName);
                myTreeResult.AddChild(subTree);
            }
            return myTreeResult;
        }
    }
}