namespace EssentialCSharp.ListingManager.Tests;

internal static class FileSystemInfoExtensions
{
    public static void DeleteReadOnly(this FileSystemInfo fileSystemInfo)
    {
        if (fileSystemInfo is DirectoryInfo directoryInfo)
        {
            foreach (FileSystemInfo childInfo in directoryInfo.GetFileSystemInfos())
            {
                childInfo.DeleteReadOnly();
            }
        }

        fileSystemInfo.Attributes = FileAttributes.Normal;
        fileSystemInfo.Refresh();
        fileSystemInfo.Delete();
    }
}