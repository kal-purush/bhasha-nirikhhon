using System;
using System.Collections.Generic;
using DAO;
using DAO.Implemenation;
using DAO.Interafaces;
using Services.Interfaces;

namespace Services.Implemenations
{
    public class BankBookService :IBankBookService
    {
        private readonly IBankBookDAO _bankBookDao = new BankBookDAO();

        public void Create(BankBookSet book)
        {
            _bankBookDao.Create(book);

        }

        public BankBookSet Get(Guid id)
        {
            return _bankBookDao.Get(id);
        }

        public void Delete(Guid id)
        {
            _bankBookDao.Delete(id);
        }

        public List<BankBookSet> GetList()
        {
            return _bankBookDao.GetList();
        }


        public void Update(BankBookSet book)
        {
            _bankBookDao.Update(book);
        }
    }
}