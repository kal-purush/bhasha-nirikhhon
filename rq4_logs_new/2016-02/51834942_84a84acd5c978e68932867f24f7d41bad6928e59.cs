using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Wunderlist.Data.Infrastructure;
using Wunderlist.Data.Repositories;
using Wunderlist.Models;
using Wunderlist.Service.Interfaces;

namespace Wunderlist.Service.Services
{
    public class ProfileService : IProfileService
    {
        private readonly IUnitOfWork uoWork;
        private readonly IProfileRepository repository;

        public ProfileService(IUnitOfWork uoW, IProfileRepository rep)
        {
            this.repository = rep;
            this.uoWork = uoW;
        }
        public void Add(UserProfile entity)
        {
            repository.Add(entity);
            uoWork.Commit();
        }

        public void Delete(int id)
        {
            repository.Delete(p=>p.ID==id);
            uoWork.Commit();
        }

        public void Delete(UserProfile entity)
        {
            var profile = GetById(entity.ID);
            repository.Delete(profile);
            uoWork.Commit();
        }

        public UserProfile GetById(int id)
        {
            return repository.GetById(id);
        }

        public void Update(UserProfile entity)
        {
            repository.Update(entity);
            uoWork.Commit();
        }
    }
}