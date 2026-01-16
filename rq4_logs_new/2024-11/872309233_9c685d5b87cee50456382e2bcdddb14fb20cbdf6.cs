namespace Data.Repository.IRepository;

public interface IUnitOfWork
{
    ICategoryRepository Category { get; }
    void Save();
}