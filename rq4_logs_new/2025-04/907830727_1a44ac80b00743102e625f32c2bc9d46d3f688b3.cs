using EasyTravel.Domain;
using EasyTravel.Domain.Entites;
using EasyTravel.Domain.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EasyTravel.Application.Services
{
    public class RecommendationService:IRecommendationService
    {
        private readonly IApplicationUnitOfWork _applicationUnitOfWork;

        public RecommendationService(IApplicationUnitOfWork applicationUnitOfWork)
        {
            _applicationUnitOfWork = applicationUnitOfWork;
        }

        public void GetRecommendation(string type, int count = 5)
        {
            _applicationUnitOfWork.RecommendationRepository.Get(type, count);        

        }

        public Task<IEnumerable<RecommendationDto>> GetRecommendationsAsync(string type, int count = 5)
        {
          return  _applicationUnitOfWork.RecommendationRepository.GetRecommendationsAsync(type, count);

        }
    }
}