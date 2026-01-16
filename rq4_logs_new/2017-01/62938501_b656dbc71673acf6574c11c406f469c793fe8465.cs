using System.Diagnostics.Contracts;
using System.IO;

namespace System
{
    public static class FileInfoExtensions
    {
        [Pure]
        public static bool ContainingDirectoryExists(this FileInfo fileInfo)
        {
            return fileInfo.Directory.Exists;
        }

        public static void EnsureContainingDirectoryExists(this FileInfo fileInfo)
        {
            if (fileInfo.ContainingDirectoryExists() == false)
                fileInfo.Directory.Create();
        }
    }

}