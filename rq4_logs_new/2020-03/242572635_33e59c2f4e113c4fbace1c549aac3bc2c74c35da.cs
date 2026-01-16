using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.Entity;
using System.Linq;
using Tholaumuntu.DataAcces.Contexts;
using Tholaumuntu.DataAcces.Domain;
using Tholaumuntu.Repository.Contracts;

namespace Tholaumuntu.Repository.Repositories
{
    public class PersonalQuizRepository : IPersonalQuizRepository
    {
        private readonly TholaUmuntuContext _tholaUmuntuContext;

        public PersonalQuizRepository()
        {
            _tholaUmuntuContext = new TholaUmuntuContext();
        }
        public int Add(PersonalQuiz quiz)
        {
            try
            {
                using (_tholaUmuntuContext)
                {
                    _tholaUmuntuContext.PersonalQuizzes.Add(quiz);
                    return _tholaUmuntuContext.SaveChanges();
                }
               
            }
            catch (DbException e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        public bool Update(PersonalQuiz quiz)
        {
            try
            {
                using (_tholaUmuntuContext)
                {
                    var result = _tholaUmuntuContext.PersonalQuizzes.SingleOrDefault(b => b.Id == quiz.Id);
                    if (result != null)
                    {
                        result = quiz;
                        _tholaUmuntuContext.Entry(result).State = EntityState.Modified;
                        return _tholaUmuntuContext.SaveChanges() > 0;
                    }
                }
            }
            catch (DbException e)
            {
                Console.WriteLine(e.Message);
                throw;
            }

            return false;
        }

        public IList<PersonalQuiz> AllQuizzes()
        {
            try
            {
                using (_tholaUmuntuContext)
                {
                    return _tholaUmuntuContext.PersonalQuizzes.ToList();
                }
            }
            catch (DbException e)
            {
                Console.WriteLine(e.Message);
                throw;
            }
        }
    }
}