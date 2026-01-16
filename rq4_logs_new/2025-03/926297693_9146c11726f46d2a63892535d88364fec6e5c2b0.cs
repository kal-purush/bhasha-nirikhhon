
using System.Collections.Generic;
using SPSS.Entities;
using SPSS.Repositories.GenericRepository;

public class AddressService
{
    private readonly IGenericRepository<Address> _repository;

    public AddressService(IGenericRepository<Address> repository)
    {
        _repository = repository;
    }

    public async Task<IEnumerable<Address>> GetAllAsync() => await _repository.GetAllAsync();
    public async Task<Address> GetByIdAsync(int id) => await _repository.GetByIdAsync(id);
    public async Task AddAsync(Address entity) => _repository.AddAsync(entity);
    public async Task UpdateAsync(Address entity) => _repository.UpdateAsync(entity);
    public async Task DeleteAsync(Address entity) => _repository.DeleteAsync(entity);
}