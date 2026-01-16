using Simply_Gallery.DAL.EF;
using Simply_Gallery.DAL.Entities;
using Simply_Gallery.DAL.Interfaces;

namespace Simply_Gallery.DAL.Repositories
{
    public class ClientManager : IClientManager
    {
        public ApplicationContext _db;

        public ClientManager(ApplicationContext db)
        {
            _db = db;
        }

        public void Create(ClientProfile profile)
        {
            _db.ClientProfiles.Add(profile);
            _db.SaveChanges();
        }

        public void Dispose()
        {
            _db.Dispose();
        }
    }
}