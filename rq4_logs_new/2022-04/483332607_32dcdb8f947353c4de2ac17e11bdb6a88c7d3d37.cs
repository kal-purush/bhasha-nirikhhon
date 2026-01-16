using System.IO;

namespace Ridavei.FileCryption.Loader.FileStream
{
    public static class FileStreamLoader
    {
        public static AFileCryptionBuilder UseFileStreamLoader(this AFileCryptionBuilder builder, FileMode fileMode = FileMode.Open, FileAccess fileAccess = FileAccess.Read, FileShare fileShare = FileShare.Read)
        {
            builder.SetFileLoaderMethod((object filePath) =>
            {
                return LoadFile(filePath.ToString(), fileMode, fileAccess, fileShare);
            });

            return builder;
        }

        private static Stream LoadFile(string filePath, FileMode fileMode, FileAccess fileAccess, FileShare fileShare)
        {
            return new System.IO.FileStream(filePath, fileMode, fileAccess, fileShare);
        }
    }
}