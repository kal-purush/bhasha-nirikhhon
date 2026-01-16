using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Inventory.DataAccessLayer;
using Inventory.Entities;
using Inventory.Exceptions;

namespace Inventory.BusinessLayer
{
    public class DistributorBL
    {
        //Validate Distributor Details
        private static bool ValidateDistributor(Distributor distributor)
        {
            StringBuilder sb = new StringBuilder();
            bool validDistributor = true;
            if (distributor.DistributorID <= 0)
            {
                validDistributor = false;
                sb.Append(Environment.NewLine + "Invalid Distributor ID");

            }
            if (distributor.DistributorName == string.Empty)
            {
                validDistributor = false;
                sb.Append(Environment.NewLine + "Distributor Name Required");

            }
            if (distributor.DistributorContactNumber.Length < 10)
            {
                validDistributor = false;
                sb.Append(Environment.NewLine + "Required 10 Digit Contact Number");
            }
            if (validDistributor == false)
                throw new InventoryException(sb.ToString());
            return validDistributor;
        }

        //Validating & Adding Distributor
        public static bool AddDistributorBL(Distributor newdistributor)
        {
            bool distributorAdded = false;
            try
            {
                if (ValidateDistributor(newdistributor))
                {
                    DistributorDAL distributorDAL = new DistributorDAL();
                    distributorAdded = distributorDAL.AddDistributorDAL(newdistributor);
                }
            }
            catch (InventoryException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return distributorAdded;
        }

        //Returning complete list of distributors
        public static List<Distributor> GetAllDistributorsBL()
        {
            List<Distributor> distributorList = null;
            try
            {
                DistributorDAL distributorDAL = new DistributorDAL();
                distributorList = distributorDAL.GetAllDistributorsDAL();
            }
            catch (InventoryException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return distributorList;
        }

        //Validating & Searching Distributor by using Distributor ID as parameter
        public static Distributor SearchDistributorBL(int searchDistributorID)
        {
            Distributor searchDistributor = null;
            try
            {
                DistributorDAL distributorDAL = new DistributorDAL();
                searchDistributor = distributorDAL.SearchDistributorDAL(searchDistributorID);
            }
            catch (InventoryException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return searchDistributor;

        }

        //Validating & Updating Distributor Details
        public static bool UpdateDistributorBL(Distributor updateDistributor)
        {
            bool distributorUpdated = false;
            try
            {
                if (ValidateDistributor(updateDistributor))
                {
                    DistributorDAL distributorDAL = new DistributorDAL();
                    distributorUpdated = distributorDAL.UpdateDistributorDAL(updateDistributor);
                }
            }
            catch (InventoryException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return distributorUpdated;
        }

        //Validating & Deleting Distributor
        public static bool DeleteDistributorBL(int deleteDistributorID)
        {
            bool distributorDeleted = false;
            try
            {
                if (deleteDistributorID > 0)
                {
                    DistributorDAL distributorDAL = new DistributorDAL();
                    distributorDeleted = distributorDAL.DeleteDistributorDAL(deleteDistributorID);
                }
                else
                {
                    throw new InventoryException("Invalid Distributor ID");
                }
            }
            catch (InventoryException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return distributorDeleted;
        }

    }
    
}