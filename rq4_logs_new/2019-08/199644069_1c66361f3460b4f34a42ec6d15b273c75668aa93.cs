using Abis.Mbs.Business.Abstract;
using Abis.Mbs.DataAccess.Abstract;
using Abis.Mbs.Entities.Concrete;
using System;
using System.Collections.Generic;
using System.Text;

namespace Abis.Mbs.Business.Concrete
{
    public class ShortAnnouncementManager : IShortAnnouncementService
    {
        private IShortAnnouncementDal _shortAnnouncementDal;

        public ShortAnnouncementManager (IShortAnnouncementDal shortAnnouncementDal)
        {
            _shortAnnouncementDal = shortAnnouncementDal;
        }

        public List<ShortAnnouncement> GetAll()
        {
            return _shortAnnouncementDal.GetList();
        }

        public List<ShortAnnouncement> GetByCategory(int ID)
        {
            return _shortAnnouncementDal.GetList(p=>p.ID==ID);
        }

        public void Add(ShortAnnouncement shortAnnouncement)
        {
            _shortAnnouncementDal.Add(shortAnnouncement);
        }

        public void Delete(int ID)
        {
            _shortAnnouncementDal.Delete(new ShortAnnouncement { ID = ID });
        }

   
        public void Update(ShortAnnouncement shortAnnouncement)
        {
            _shortAnnouncementDal.Update(shortAnnouncement);
        }

        public ShortAnnouncement GetById(int ID)
        {
            return _shortAnnouncementDal.Get(p => p.ID == ID);
        }
    }
}