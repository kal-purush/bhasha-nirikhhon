using ISM.Application.Interfaces.Suppliers;
using ISM.Domain.Models;
using ISM.Infrastructure.ISMDbcontext;
using ISM.Infrastructure.Validation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ISM.Infrastructure.Repositories
{
    public class RepositoryForSuppliers : ISupplierRepository
    {
        private ISMdbcontext _dbcontext;
        private ISupplierValidation _supplierValidation;
        public RepositoryForSuppliers()
        {
            _dbcontext = new ISMdbcontext();
            _supplierValidation = new ValidationForSuppliers();
        }
        public Supplier Create(Supplier Objectname)
        {
            if (_supplierValidation.Create(Objectname) == true)
            {
                _dbcontext.Suppliers.Add(Objectname);
                _dbcontext.SaveChanges();
            }
            return null;
        }

        public int Delete(int id)
        {
            if (_supplierValidation.Delete(id) == true)
            {
                Supplier? supplier = _dbcontext.Suppliers.FirstOrDefault(i => i.Id == id);
                _dbcontext.Suppliers.Remove(supplier);
                _dbcontext.SaveChanges();
                return id;
            }
            return 0;
        }

        public IEnumerable<Supplier> GetAll()
        {
            if (_supplierValidation.Getall() == true)
            {
                return _dbcontext.Suppliers.ToList();
            }
            return null;
        }

        public Supplier Getbyid(int id)
        {
            if (_supplierValidation.Getall() == true)
            {
                return _dbcontext.Suppliers.FirstOrDefault(x => x.Id == id);
            }
            return null;
        }

        public bool Update(Supplier objectname)
        {
            if (_supplierValidation.Update(objectname) == true)
            {
                _dbcontext.Suppliers.Update(objectname);
                _dbcontext.SaveChanges();
                return true;
            }
            return false;
        }
    }
}