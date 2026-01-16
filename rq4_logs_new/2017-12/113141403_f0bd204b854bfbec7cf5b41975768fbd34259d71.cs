using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAccess
{
    public class UploadRepository
    {
        Datastore datastore;
        string[] firstNames = { "Tyler", "Grant", "Brian", "Zed", "Terry", "Connor" };
        string[] lastNames = { "Bigglesworth", "MacLeod", "Hranka", "Hall", "Holmes" };

        public UploadRepository()
        {
            datastore = new Datastore(ConfigurationManager.ConnectionStrings["fpc-workshop2-database"].ConnectionString);
        }

        public IEnumerable<Upload> ReadUploads()
        {
            var query = datastore.Uploads
                .Include("User")
                .OrderBy(upload => upload.Created);

            return query.ToList();
        }

        public void InsertUpload(string payload)
        {
            datastore.Uploads.Add(new Upload()
            {
                Payload = payload,
                Created = DateTime.Now,
                Updated = DateTime.Now,
                // Create a randome user too.
                User = new User()
                {
                    FirstName = firstNames[new Random().Next(0, firstNames.Length)],
                    LastName = lastNames[new Random().Next(0, lastNames.Length)],
                    Created = DateTime.Now,
                    Updated = DateTime.Now
                }
            });

            datastore.SaveChanges();
        }
    }
}