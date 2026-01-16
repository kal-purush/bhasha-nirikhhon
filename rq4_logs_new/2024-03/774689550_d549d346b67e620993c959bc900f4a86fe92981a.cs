using DataAccess.CRUD;
using DTO;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CoreApp
{
    public class ServiceManager
    {
        public void Create(Service service)
        {
            var sc = new ServiceCrudFactory();


            if (!IsValidName(service.Name))
            {
                throw new Exception("Invalid name format");
            }
            else if (!IsValidDesc(service.Description))
            {
                throw new Exception("Invalid description format");
            }
            else if (!IsValidPrice(service.Price))
            {
                throw new Exception("Invalid price format");
            }

            sc.Create(service);
        }

        public List<T> RetrieveAll<T>()
        {
            var sc = new ServiceCrudFactory();
            return sc.RetrieveAll<T>();
        }

        public Service RetrieveById(int ID)
        {
            var sc = new ServiceCrudFactory();
            return sc.RetrieveById<Service>(ID);
        }


        public void Update(Service service)
        {
            var sc = new ServiceCrudFactory();

            if (!IsValidName(service.Name))
            {
                throw new Exception("Invalid name format");
            }
            else if (!IsValidDesc(service.Description))
            {
                throw new Exception("Invalid description format");
            }
            else if (!IsValidPrice(service.Price))
            {
                throw new Exception("Invalid price format");
            }
            else if (!IsValidID(service.BranchID))
            {
                throw new Exception("Invalid Branch ID");
            }
            sc.Update(service);
        }

        public void Delete(Service service)
        {
            var sc = new ServiceCrudFactory();
            sc.Delete(service);
        }

        private bool IsValidName(string name)
        {
            return !string.IsNullOrWhiteSpace(name) && char.IsUpper(name[0]) && name.All(c => char.IsLetter(c) && !char.IsWhiteSpace(c));
        }

        private bool IsValidPrice(int price)
        {
            return price > 0;
        }

        private bool IsValidDesc(string desc)
        {
            return !string.IsNullOrWhiteSpace(desc);
        }


        private bool IsValidID(int id)
        {
            return id > 0;
        }
    }
}