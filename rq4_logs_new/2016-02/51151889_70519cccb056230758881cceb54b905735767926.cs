namespace BoardGames.Services.Data
{
    using Contracts;
    using BoardGames.Data.Models;
    using BoardGames.Data.Common;

    public class FilesService : IFilesService
    {
        private readonly IDbRepository<File> files;

        public FilesService(IDbRepository<File> files)
        {
            this.files = files;
        }

        public int Add(string fileType, byte[] content)
        {
            var newFile = new File
            {
                ContentType = fileType,
                Content = content
            };

            this.files.Add(newFile);
            this.files.Save();

            return newFile.Id;
        }

        public int Edit(int id, string fileType, byte[] content)
        {
            var fileToBeEdited = this.files.GetById(id);

            fileToBeEdited.ContentType = fileType;
            fileToBeEdited.Content = content;

            this.files.Save();

            return fileToBeEdited.Id;
        }

        public File GetById(int id)
        {
            return this.files.GetById(id);
        }
    }
}