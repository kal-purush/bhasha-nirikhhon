using AppVecinos.API.Data;
using AppVecinos.API.Models;

namespace AppVecinos.API.Services
{
    public class BalanceService : IBalanceService
    {
        private readonly IUnitOfWork _unitOfWork;

        public BalanceService(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        public async Task<IEnumerable<Balance>> GetBalancesAsync()
        {
            return await _unitOfWork.BalanceRepository.GetAllAsync();
        }

        public async Task<Balance> CreateBalanceAsync(Balance balance)
        {
            await _unitOfWork.BalanceRepository.AddAsync(balance);
            await _unitOfWork.SaveAsync();
            return balance;
        }

        public async Task<IEnumerable<Balance>> GetBalancesByYearAsync(string year)
        {
            var results = (await _unitOfWork.BalanceRepository.GetAllAsync())
                          .Where(p => p.Year.ToString() == year).ToList();

            return results;
        }

        public async Task<IEnumerable<Balance>> GetBalancesByPeriodAsync(string period)
        {
            var results = (await _unitOfWork.BalanceRepository.GetAllAsync())
                          .Where(p => p.Period.ToString() == period).ToList();
            return results;
        }
    }
}