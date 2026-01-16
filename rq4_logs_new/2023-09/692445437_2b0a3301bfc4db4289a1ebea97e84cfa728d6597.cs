using ISM.Application.Interfaces.Materials;
using ISM.Domain.Models;
using ISM.Infrastructure.ISMDbcontext;
using ISM.Infrastructure.Validation;
namespace ISM.Infrastructure.Repositories
{
    public class RepositoryforMaterial:IMaterialRepository
    {

        private ISMdbcontext _dbcontext;

        private IMaterialValidation _validation;
        public RepositoryforMaterial()
        {
            _dbcontext = new ISMdbcontext();
            _validation = new ValidationforMaterial();
        }
        public Material Create(Material Objectname)
        {
            if (_validation.Create(Objectname) == true)
            {
                _dbcontext.Materials.Add(Objectname);
                _dbcontext.SaveChanges();
                return Objectname;
            }
            return null;
        }

        public int Delete(int id)
        {
            if (_validation.Delete(id) == true)
            {
                Material? MaterialDelete = _dbcontext.Materials.Find(id);
                _dbcontext.Materials.Remove(MaterialDelete);
                _dbcontext.SaveChanges();
                return id;
            }
            return 0;
        }

        public IEnumerable<Material> GetAll()
        {

            if (_validation.Getall() == true)
                return _dbcontext.Materials.ToList();
            return null;
        }

        public Material Getbyid(int id)
        {
            if (_validation.Getby(id) == true)
                return _dbcontext.Materials.Find(id);
            return null;
        }

        public bool Update(Material objectname)
        {
            if (_validation.Update(objectname) == true)
            {
                _dbcontext.Materials.Update(objectname);
                _dbcontext.SaveChanges();
                return true;
            }
            return false;
        }
    }
}