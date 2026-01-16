using Microsoft.AspNetCore.Mvc;

namespace HealthMate.API.Abstractions
{
    public interface IGenericController<in TShorDto>
    {
        public Task<IActionResult> GetByIdAsync(Guid id, CancellationToken token);
        public Task<IActionResult> GetAllAsync(CancellationToken token);
        public Task<IActionResult> UpdateAsync(Guid id, TShorDto model, CancellationToken token);
        public Task<IActionResult> DeleteAsync(Guid id, CancellationToken token);
        public Task<IActionResult> CreateAsync(TShorDto model, CancellationToken token);
    }
}