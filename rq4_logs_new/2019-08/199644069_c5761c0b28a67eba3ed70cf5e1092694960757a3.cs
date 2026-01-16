using Abis.Mbs.Business.Abstract;
using Abis.Mbs.DataAccess.Abstract;
using Abis.Mbs.Entities.Concrete;
using System;
using System.Collections.Generic;
using System.Text;

namespace Abis.Mbs.Business.Concrete
{
     public class AnnouncementManager : IAnnouncementService
    {
        private IAnnouncementDal _announcementDal;

        public AnnouncementManager(IAnnouncementDal announcementDal)
        {
            _announcementDal = announcementDal; 
        }
        public void Add(Announcement announcement)
        {
            _announcementDal.Add(announcement);
        }

       

        public void Delete(int announcementId)
        {
            _announcementDal.Delete(new Announcement { AnnouncementId = announcementId });
        }

        public List<Announcement> GetAll()
        {
            return _announcementDal.GetList();
        }

        public Announcement GetById(int announcementId)
        {
            return _announcementDal.Get(p => p.AnnouncementId == announcementId);
        }

        public void Update(Announcement announcement)
        {
            _announcementDal.Update(announcement);
        }

       

       
    }
}