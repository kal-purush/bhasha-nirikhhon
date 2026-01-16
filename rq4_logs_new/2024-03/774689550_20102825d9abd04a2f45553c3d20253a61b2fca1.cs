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

    //Clase de negocio donde se aplican las validaciones funcionales 

    public class BranchManager
    {
        
            //Metodo para create


            public void Create(Branch branch)
            {
                var uc = new BranchCrudFactory();

                //Valiacion de forma

                if (!IsValidName(branch.Name))
                {
                    throw new Exception("Invalid name format");
                }
                else if (!IsValidAddress(branch.Address))
                {
                    throw new Exception("Invalid Address format");
                }
                else if (!IsValidDescription(branch.Description))
                {
                    throw new Exception("Invalid Description format");
                }
                uc.Create(branch);






            }

            public List<Branch> RetrieveAll()
            {
                var uc = new BranchCrudFactory();
                return uc.RetrieveAll<Branch>();
            }

            public Branch RetrieveById(int branchId)
            {
                var uc = new BranchCrudFactory();
                return uc.RetrieveById<Branch>(branchId);
            }


            public void Update(Branch branch)
            {
                var uc = new BranchCrudFactory();

                if (!IsValidName(branch.Name))
                {
                    throw new Exception("Invalid name format");
                }
                else if (!IsValidAddress(branch.Address))
                 {
                throw new Exception("Invalid Address format");
                 }
                 else if (!IsValidDescription(branch.Description))
                 {
                throw new Exception("Invalid Description format");
                 }
            uc.Update(branch);

            }

            public void Delete(Branch branch)
            {
                var uc = new BranchCrudFactory();
                uc.Delete(branch);
            }

            private bool IsValidName(string name)
            {
                return !string.IsNullOrWhiteSpace(name) && char.IsUpper(name[0]) && name.All(c => char.IsLetter(c) && !char.IsWhiteSpace(c));
            }

            private bool IsValidAddress(string address)
            {
                return !string.IsNullOrWhiteSpace(address) && char.IsUpper(address[0]) && address.All(c => char.IsLetter(c) && !char.IsWhiteSpace(c));
            }

        private bool IsValidDescription(string description)
        {
            return !string.IsNullOrWhiteSpace(description) && char.IsUpper(description[0]) && description.All(c => char.IsLetter(c) && !char.IsWhiteSpace(c));
        }


        }
    }
