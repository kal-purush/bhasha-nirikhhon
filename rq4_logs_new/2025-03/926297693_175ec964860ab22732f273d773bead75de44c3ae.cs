using SPSS.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPSS.Service.Services.PaymentService
{
    public interface IPaymentService
    {
        Task<IEnumerable<Payment>> GetAllAsync();
        Task AddPaymentAsync(Payment payment);
        Task UpdatePaymentAsync(int id, Payment payment);
        Task<Payment> GetPaymentByIdAsync(int id);
        Task<Payment?> GetPaymentByOrderIdAsync(int orderId);
    }
}