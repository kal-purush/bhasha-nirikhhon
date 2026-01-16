using OnlineMarks.Data.Models;
using OnlineMarks.Data.Models.Context;
using OnlineMarks.Interfaces.Repository;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace OnlineMarks.Data.Repositories
{
    public class SubjectRepository : ISubjectRepository
    {
        private readonly ApplicationContext _applicationContext;
        public SubjectRepository(ApplicationContext applicationContext)
        {
            _applicationContext = applicationContext;
        }

        public void Add(Subject subject)
        {
            _applicationContext.Subjects.Add(subject);
            _applicationContext.SaveChanges();
        }

        public Subject Get(Guid subjectId)
        {
            return _applicationContext.Subjects.FirstOrDefault(x => x.Id == subjectId);
        }

        public List<Subject> GetAll()
        {
            return _applicationContext.Subjects.ToList();
        }
    }
}