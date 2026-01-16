using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Online_Survey.DTOs;
using Online_Survey.Models;

namespace Online_Survey.Data
{
    public class UserRepository : IUserRepository
    {
        private readonly InternshipOnlineSurveyContext _ef;

        public UserRepository(IConfiguration config)
        {
            _ef = new InternshipOnlineSurveyContext(config);
        }

        bool IUserRepository.SaveChange()
        {
            return _ef.SaveChanges() > 0;
        }
        
        void IUserRepository.AddEntity<T>(T data)
        {
            if(data!=null)
            {
                _ef.Add(data);
            }
        }
    }
}