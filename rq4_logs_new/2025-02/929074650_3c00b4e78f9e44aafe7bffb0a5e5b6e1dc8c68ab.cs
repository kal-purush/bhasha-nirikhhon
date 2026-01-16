using System;
using Core.Entities;

namespace Core.Interfaces;

public interface IGenericRepository <T> where T : BaseEntity
{
    Task<T?> GetByIdAsync(int id);
    
    //specification for the get types and get brands
    Task<T?> GetEntityWithSpec(ISpecification<T> spec);
    Task<IReadOnlyList<T?>>ListAsync(ISpecification<T> spec);

    //Same versions but this supports TResult (Different type parameters)
    Task<TResult?> GetEntityWithSpec<TResult>(ISpecification<T, TResult> spec);
    Task<IReadOnlyList<TResult>>ListAsync<TResult>(ISpecification<T, TResult> spec);

    //Distint section
    


    Task<IReadOnlyList<T>> ListAllAsync();
    void Add(T entity);
    void Update(T entity);
    void Remove(T entity);
    Task<bool> SaveAllAsync();
    bool Exists(int id);
    

}
