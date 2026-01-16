
using System.Collections.Generic;
using SPSS.Entities;
using SPSS.Repositories.GenericRepository;

public class PaymentStatusService (IGenericRepository<PaymentStatus> repository)
{
    public async Task<IEnumerable<PaymentStatus>> GetAllAsync() => await repository.GetAllAsync();
    public async Task<PaymentStatus> GetByIdAsync(int id) => await repository.GetByIdAsync(id);
    public async Task AddAsync(PaymentStatus entity) => repository.AddAsync(entity);
    public async Task UpdateAsync(PaymentStatus entity) => repository.UpdateAsync(entity);
    public async Task DeleteAsync(PaymentStatus entity) => repository.DeleteAsync(entity);
}