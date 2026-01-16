using api.Models;

namespace api.interfaces;

public  interface IStockRepository
{
    Task<List<Stock>> GetAllAsync();
    
}