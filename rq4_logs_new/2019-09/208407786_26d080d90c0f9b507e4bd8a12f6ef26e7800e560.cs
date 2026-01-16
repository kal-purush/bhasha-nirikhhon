using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Inventory.Entities;
using Inventory.DataAccessLayer;
using System.Text.RegularExpressions;
using Inventory.Exception;

namespace Inventory.BusinessLayer
{
    public class RawMaterialBL
    {
        private static bool ValidateRawMaterial(RawMaterial rawMaterial)
        {
            StringBuilder sb = new StringBuilder();
            bool validRawMaterial = true;
            if (rawMaterial.RawMaterialID == 0 || rawMaterial.RawMaterialID > 99999)
            {
                validRawMaterial = false;
                sb.Append("\nInvalid Raw Material ID");
            }
            Regex regex = new Regex("^[a-zA-Z]+$");
            if (!regex.IsMatch(rawMaterial.RawMaterialName) || rawMaterial.RawMaterialName == String.Empty || rawMaterial.RawMaterialName.Length > 30)
            {
                validRawMaterial = false;
                sb.Append("\nInvalid Raw Material Name");
            }
            if (rawMaterial.RawMaterialUnitPrice <= 0)
            {
                validRawMaterial = false;
                sb.Append("\nInvalid Raw Material Price");
            }
            if (validRawMaterial == false)
            {
                throw new InventoryException(sb.ToString());
            }
            return validRawMaterial;
        }

        public static bool AddRawMaterialBL(RawMaterial newRawMaterial)
        {
            bool rawMaterialAdded = false;
            try
            {
                if (ValidateRawMaterial(newRawMaterial))
                {
                    RawMaterialDAL rawMaterialDAL = new RawMaterialDAL();
                    rawMaterialAdded = rawMaterialDAL.AddRawMaterialDAL(newRawMaterial);
                }
                else
                {
                    throw new InventoryException("Invalid Raw Material Detail");
                }
            }
            catch (InventoryException)
            {
                throw;
            }
            return rawMaterialAdded;
        }

        public static bool DeleteRawMaterialBL(int deleteRawMaterialID)
        {
            bool rawMaterialDeleted = false;
            try
            {
                if (deleteRawMaterialID > 0 && deleteRawMaterialID < 99999)
                {
                    RawMaterialDAL rawMaterialDAL = new RawMaterialDAL();
                    rawMaterialDeleted = rawMaterialDAL.DeleteRawMaterialDAL(deleteRawMaterialID);
                }
                else
                {
                    throw new InventoryException("Invalid Raw Material ID");
                }

            }
            catch (InventoryException)
            {
                throw;
            }
            return rawMaterialDeleted;
        }

        public static List<RawMaterial> GetAllRawMaterialsBL()
        {
            List<RawMaterial> rawMaterialList = null;
            try
            {
                RawMaterialDAL rawMaterialDAL = new RawMaterialDAL();
                rawMaterialList = rawMaterialDAL.GetAllRawMaterialsDAL();
            }
            catch (InventoryException ex)
            {
                throw ex;
            }
            return rawMaterialList;
        }

        public static RawMaterial SearchRawMaterialByIDBL(int searchRawMaterialID)
        {
            RawMaterial searchRawMaterial = null;
            try
            {
                RawMaterialDAL rawMaterialDAL = new RawMaterialDAL();
                searchRawMaterial = rawMaterialDAL.SearchRawMaterialDAL(searchRawMaterialID);
            }
            catch (InventoryException ex)
            {
                throw ex;
            }
            return searchRawMaterial;
        }

        public static List<RawMaterial> SearchRawMaterialByNameBL(string searchRawMaterialName)
        {
            List<RawMaterial> searchRawMaterial = null;
            try
            {
                RawMaterialDAL rawMaterialDAL = new RawMaterialDAL();
                searchRawMaterial = rawMaterialDAL.GetRawMaterialByNameDAL(searchRawMaterialName);
            }
            catch (InventoryException ex)
            {
                throw ex;
            }
            return searchRawMaterial;
        }

    }
}