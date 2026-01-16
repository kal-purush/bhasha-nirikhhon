using MAJServices.Entities;
using MAJServices.Services.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MAJServices.Services.InterfacesImplementation
{
    public class FileInfoRepository : IFileInfoRepository
    {
        private InfoContext _infoContext;

        public FileInfoRepository(InfoContext infoContext)
        {
            _infoContext = infoContext;
        }

        public void AddFile(FilePath file)
        {
            _infoContext.FilePaths.Add(file);
        }

        public bool SaveFile()
        {
            return (_infoContext.SaveChanges() >= 0);
        }
    }
}