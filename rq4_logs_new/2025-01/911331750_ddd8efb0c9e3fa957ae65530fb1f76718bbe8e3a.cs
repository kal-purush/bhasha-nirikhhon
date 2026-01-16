using AppVecinos.API.Data;
using AppVecinos.API.Models;
using Microsoft.EntityFrameworkCore;
using System.Linq.Expressions;

namespace AppVecinos.API.Services
{
    public class PaymentService : IPaymentService
    {
       private readonly IUnitOfWork _unitOfWork;

        public PaymentService(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        public async Task<IEnumerable<Payment>> GetPaymentsAsync()
        {
            return await _unitOfWork.PaymentRepository.GetAllAsync();
        }

        public async Task<Payment> CreatePaymentAsync(Payment payment)
        {
            await _unitOfWork.PaymentRepository.AddAsync(payment);
            await _unitOfWork.SaveAsync();
            return payment;
        }

        public async Task<IEnumerable<Payment>> GetPaymentsByNeighborIdAsync(int neighborId)
        {
            var results = (await _unitOfWork.PaymentRepository.GetAllAsync())
                          .Where(p => p.NeighborId == neighborId).ToList();

            return results;
        }

        public async Task<IEnumerable<Payment>> GetPaymentsByFeeIdAsync(int feeId)
        {
            var results = (await _unitOfWork.PaymentRepository.GetAllAsync())
                          .Where(p => p.FeeId == feeId).ToList(); 
            return results;
        }

        public async Task<IEnumerable<Payment>> GetPaymentsByDateAsync(DateTime dateTime)
        {
            var results = (await _unitOfWork.PaymentRepository.GetAllAsync())
                          .Where(p => p.Date.Date == dateTime.Date).ToList();  
            return results;
        }
    }
}