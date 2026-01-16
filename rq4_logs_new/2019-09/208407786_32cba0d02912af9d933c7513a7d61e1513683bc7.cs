using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Inventory.Entities;
using Inventory.Exceptions;
using Inventory.DataAccessLayer;

namespace Inventory.BusinessLayer
{
    public class SupplierAddressBL
    {
        private static bool ValidateSupplierAddress(SupplierAddress supplierAddress)
        {
            StringBuilder sb = new StringBuilder();
            bool validSupplierAddress = true;
            if (supplierAddress.SupplierId == null)
            {
                validSupplierAddress = false;
                sb.Append(Environment.NewLine + "Invalid Supplier ID");

            }
            if (supplierAddress.SupplierAddressLine1 == string.Empty)
            {
                validSupplierAddress = false;
                sb.Append(Environment.NewLine + "Supplier Address Required");

            }
            if (supplierAddress.SupplierCity == null)
            {
                validSupplierAddress = false;
                sb.Append(Environment.NewLine + "Supplier City required");
            }
            if (supplierAddress.SupplierState == null)
            {
                validSupplierAddress = false;
                sb.Append(Environment.NewLine + "Supplier State required");
            }
            if (supplierAddress.SupplierPin == null)
            {
                validSupplierAddress = false;
                sb.Append(Environment.NewLine + "Supplier Pin required");
            }
            if (validSupplierAddress == false)
                throw new Exception(sb.ToString());
            return validSupplierAddress;
        }

        public static bool AddSupplierAddressBL(SupplierAddress newSupplierAddress)
        {
            bool SupplierAddressAdded = false;
            try
            {
                if (ValidateSupplierAddress(newSupplierAddress))
                {
                    SupplierAddressDAL supplierDAL = new SupplierAddressDAL();
                    SupplierAddressAdded = supplierDAL.AddSupplierAddressDAL(newSupplierAddress);
                }
            }
            catch (Exception)
            {
                throw;
            }

            return SupplierAddressAdded;
        }

        public static List<Supplier> GetAllSupplierBL()
        {
            List<Supplier> SupplierList = null;
            try
            {
                SupplierDAL supplierDAL = new SupplierDAL();
                SupplierList = supplierDAL.GetAllSupplierDAL();
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return SupplierList;
        }

        public static SupplierAddress SearchSupplierAddressBL(int searchSupplierID)
        {
            SupplierAddress searchSupplierAddress = null;
            try
            {
                SupplierAddressDAL supplierAddressDAL = new SupplierAddressDAL();
                searchSupplierAddress = supplierAddressDAL.SearchSupplierAddressDAL(searchSupplierID);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return searchSupplierAddress;

        }

        public static bool UpdateSupplierAddressBL(SupplierAddress updateSupplierAddress)
        {
            bool SupplierAddressUpdated = false;
            try
            {
                if (ValidateSupplierAddress(updateSupplierAddress))
                {
                    SupplierAddressDAL supplierAddressDAL = new SupplierAddressDAL();
                    SupplierAddressUpdated = supplierAddressDAL.UpdateSupplierAddressDAL(updateSupplierAddress);
                }
            }
            catch (Exception)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return SupplierAddressUpdated;
        }

        public static bool DeleteSupplierAddressBL(string deleteSupplierAddressID)
        {
            bool SupplierAddressDeleted = false;
            try
            {
                if (deleteSupplierAddressID == null)
                {
                    SupplierAddressDAL supplierAddressDAL = new SupplierAddressDAL();
                    SupplieAddressDeleted = supplierAddressDAL.DeleteSupplierAddressDAL(int.Parse(deleteSupplierAddressID));
                }
                else
                {
                    throw new Exception("Invalid Guest ID");
                }
            }
            catch (Exception)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return SupplierAddressDeleted;
        }

    }
}