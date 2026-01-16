using AppVecinos.API.Data;
using AppVecinos.API.Models;

namespace AppVecinos.API.Services
{
    public class OutcomeService : IOutcomeService
    {
        private readonly IUnitOfWork _unitOfWork;

        public OutcomeService(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        public async Task<IEnumerable<Outcome>> GetOutcomesAsync()
        {
            return await _unitOfWork.OutcomeRepository.GetAllAsync();
        }

        public async Task<Outcome> CreateOutcomeAsync(Outcome outcome)
        {
            await _unitOfWork.OutcomeRepository.AddAsync(outcome);
            await _unitOfWork.SaveAsync();
            return outcome;
        }

        public async Task<IEnumerable<Outcome>> GetOutcomesByYearAsync(string year)
        {
            var results = (await _unitOfWork.OutcomeRepository.GetAllAsync())
                          .Where(p => p.Year.ToString() == year).ToList();

            return results;
        }

        public async Task<IEnumerable<Outcome>> GetOutcomesByMonthAsync(string month)
        {
            var results = (await _unitOfWork.OutcomeRepository.GetAllAsync())
                          .Where(p => p.Month.ToString() == month).ToList();
            return results;
        }
    }
}