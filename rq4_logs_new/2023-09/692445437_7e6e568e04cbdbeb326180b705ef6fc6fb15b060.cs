using ISM.Application.Interfaces.Storage;
using ISM.Application.Interfaces.Storages;
using ISM.Domain.Models;
using ISM.Infrastructure.ISMDbcontext;
using ISM.Infrastructure.Validation;

namespace ISM.Infrastructure.Repositories;


public class RepositoryforStorage : IStorageRepository
{
    private ISMdbcontext _dbcontext;

    private IStorageValidation _validation;
    public RepositoryforStorage()
    {
        _dbcontext = new ISMdbcontext();
        _validation = new ValidationForStorage();
    }
    public Storage Create(Storage Objectname)
    {
        if (_validation.Create(Objectname) == true)
        {
            _dbcontext.Storages.Add(Objectname);
            _dbcontext.SaveChanges();
            return Objectname;
        }
        return null;
    }

    public int Delete(int id)
    {
        if (_validation.Delete(id) == true)
        {
            Storage? StorageDelete = _dbcontext.Storages.Find(id);
            _dbcontext.Storages.Remove(StorageDelete);
            _dbcontext.SaveChanges();
            return id;
        }
        return 0;
    }

    public IEnumerable<Storage> GetAll()
    {
        if (_validation.Getall() == true)
            return _dbcontext.Storages.ToList();
        return null;
    }

    public Storage Getbyid(int id)
    {
        if (_validation.Getby(id) == true)
            return _dbcontext.Storages.Find(id);
        return null;
    }

    public bool Update(Storage objectname)
    {
        if (_validation.Update(objectname) == true)
        {
            _dbcontext.Storages.Update(objectname);
            _dbcontext.SaveChanges();
            return true;
        }
        return false;
    }
}
