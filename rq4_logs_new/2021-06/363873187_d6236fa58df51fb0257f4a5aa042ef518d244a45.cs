using System;
using System.Collections.Generic;
using CSB.Business.Interfaces;
using CSB.Repository.Entities;
using CSB.Repository.Interfaces;

namespace CSB.Business.Impl

{
    public class PhoneService : IPhoneService
    {
        private readonly IPhoneRepository _phoneRepository;

        public PhoneService(IPhoneRepository phoneRepository)
        {
            this._phoneRepository = phoneRepository;
        }
        public int Create(Phone phone)
        {
            if (phone is null)
            {
                throw new ArgumentNullException();
            }
            
            return _phoneRepository.Create(phone);
        }

        public bool Update(Phone phone)
        {
            if (phone is null)
            {
                throw new ArgumentNullException();
            }
            return _phoneRepository.Update(phone);
        }

        public bool Delete(int id)
        {
            
            if (id <= 0)
            {
                throw new ArgumentException();
            }
            return _phoneRepository.Delete(id);
           
        }

        public IReadOnlyCollection<Phone> GetAll()
        {
            return _phoneRepository.GetAll();
        }


        public Phone GetById(int id)
        {
            if (id <= 0)
            {
                throw new ArgumentException();
            }
            return _phoneRepository.GetById(id);
        }
    }
}