using Application.Abstractions.Data;
using Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace Persistence.Repositories;

public class ProductPhaseSalaryRepository : IProductPhaseSalaryRepository
{
    private readonly AppDbContext _context;
    public ProductPhaseSalaryRepository(AppDbContext context)
    {
        _context = context;
    }
    public void Add(ProductPhaseSalary productPhaseSalary)
    {
        _context.ProductPhaseSalaries.Add(productPhaseSalary);
    }

    public void AddRange(List<ProductPhaseSalary> productPhaseSalaries)
    {
        _context.ProductPhaseSalaries.AddRange(productPhaseSalaries);
    }

    public async Task<ProductPhaseSalary> GetByProductIdAndPhaseId(Guid productId, Guid phaseId)
    {
        return await _context.ProductPhaseSalaries.SingleOrDefaultAsync(x => x.ProductId == productId && x.PhaseId == phaseId);
    }

    public void UpdateRange(List<ProductPhaseSalary> productPhaseSalaries)
    {
        _context.ProductPhaseSalaries.UpdateRange(productPhaseSalaries);
    }
}