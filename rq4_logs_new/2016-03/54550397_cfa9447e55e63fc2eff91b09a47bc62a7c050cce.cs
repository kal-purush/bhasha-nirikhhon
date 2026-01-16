using IdeasRepository.BL.Interfaces;
using IdeasRepository.DAL.Contexts;
using IdeasRepository.DAL.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IdeasRepository.BL.Providers
{
    public class RecordsProvider : IRecordsProvider
    {
        private ApplicationDbContext _context;

        public RecordsProvider()
        {
            _context = new ApplicationDbContext();
        }

        public List<Record> GetAllRecords()
        {
            throw new NotImplementedException();
        }

        public List<RecordType> GetAllRecordTypes()
        {
            throw new NotImplementedException();
        }

        public Record GetRecord(string id)
        {
            throw new NotImplementedException();
        }

        public void AddRecord(Record record)
        {
            throw new NotImplementedException();
        }

        public void UpdateRecord(Record record)
        {
            throw new NotImplementedException();
        }

        public void RemoveRecord(Record record)
        {
            throw new NotImplementedException();
        }
    }
}