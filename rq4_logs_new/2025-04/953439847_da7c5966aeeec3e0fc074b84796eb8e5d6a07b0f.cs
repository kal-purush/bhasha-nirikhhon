using BuildingBlocks.Application.Ports;

namespace Infrastructure
{
    public class Storage : IStorage
    {
        public Task<StorageFile> GetFile(string path, string fileName)
        {
            using var fs = File.Open(path, FileMode.Open, FileAccess.Read);
            using var reader = new BinaryReader(fs);
            var bytes = reader.ReadBytes((int)fs.Length);
            var storageFile = new StorageFile(fileName, bytes);
            return Task.FromResult(storageFile);
        }

        public Task<IEnumerable<string>> GetFilesNames(string path, string extension)
        {
            DirectoryInfo dir = new DirectoryInfo(path);
            var files = dir.GetFiles($"*.{extension}");
            return Task.FromResult(files.Select(f => f.Name));
        }

        public Task<bool> SaveFile(string path, string fileName, byte[] content)
        {
            using var fs = File.Create(Path.Combine(path, fileName));
            using var writer = new BinaryWriter(fs);
            writer.Write(content);
            return Task.FromResult(true);
        }
    }
}