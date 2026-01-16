using Entities;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Data.DataModels
{
    public class FileDataModel : IFileDataModel
    {
        public List<File> Get()
        {
            using (var context = new FileUploadEntities())
            {
                return context.Files.ToList();
            }
        }

        public List<File> Get(string userID)
        {
            using (var context = new FileUploadEntities())
            {
                return context.Files.Where(x => x.UserID == userID).ToList();
            }
        }

        public File Get(Guid id)
        {
            using (var context = new FileUploadEntities())
            {
                return context.Files.FirstOrDefault(x => x.ID == id);
            }
        }

        public bool Exists(string filename, string fileExtension)
        {
            using (var context = new FileUploadEntities())
            {
                var file = context.Files.FirstOrDefault(x => x.Filename == filename && x.FileExtension == fileExtension);
                return file == null ? false : true;
            }
        }

        public bool Exists(Guid id)
        {
            using (var context = new FileUploadEntities())
            {
                Guid? searchId = context.Files.Select(x => x.ID).FirstOrDefault(x => x == id);
                return searchId == null ? false : true;
            }
        }

        public File Insert(File file)
        {
            using (var context = new FileUploadEntities())
            {

                context.Files.Add(file);
                context.SaveChanges();
                return file;
            }
        }

        public int Update(File file)
        {
            using (var context = new FileUploadEntities())
            {
                context.Entry(file).State = EntityState.Modified;
                return context.SaveChanges();
            }
        }

        public bool Delete(Guid id)
        {
            using (var context = new FileUploadEntities())
            {
                var value = context.Files.FirstOrDefault(x => x.ID == id);

                if (value != null)
                {
                    context.Files.Remove(value);
                    context.SaveChanges();
                    return true;
                }

                return false;
            }
        }
    }
}