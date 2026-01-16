using System;
using System.Collections.Generic;
using System.Linq;
using DAO.Interafaces;

namespace DAO.Implemenation
{
    // ReSharper disable once InconsistentNaming
    public class RequestDAO : IRequestDAO
    {
        private readonly LalkaBankDabaseModelContainer _db = new LalkaBankDabaseModelContainer();

        public void Create(RequestSet request)
        {
            _db.RequestSets.Add(request);
            _db.SaveChanges();
        }

        public RequestSet Get(Guid id)
        {
            var request = _db.RequestSets.Find(id);
            if (request == null) { throw new Exception("not found"); }

            return request;
        }

        public void Delete(Guid id)
        {
            var request = _db.RequestSets.Find(id);
            if (request == null) { throw new Exception("not found"); }

            _db.RequestSets.Remove(request);
            _db.SaveChanges();
        }

        public List<RequestSet> GetList()
        {
            return _db.RequestSets.ToList();
        }
    }
}