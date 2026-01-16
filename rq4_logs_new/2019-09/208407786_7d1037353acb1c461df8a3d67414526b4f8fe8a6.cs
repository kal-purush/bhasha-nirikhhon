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
    class DistributorAddressBL
    {
        //Validate Distributor Address Details
        private static bool ValidateDistributorAddress(DistributorAddress distributorAddress)
        {
            StringBuilder sb = new StringBuilder();
            bool validDistributorAddress = true;
            if (distributorAddress.DistributorAddressID <=0)
            {
                validDistributorAddress = false;
                sb.Append(Environment.NewLine + "Invalid Distributor ID");

            }
            if (distributorAddress.DistributorAddressLine1 == string.Empty)
            {
                validDistributorAddress = false;
                sb.Append(Environment.NewLine + "Address Line1 Required");

            }
            if (distributorAddress.DistributorAddressLine2 == string.Empty)
            {
                validDistributorAddress = false;
                sb.Append(Environment.NewLine + "Address Line2 Required");

            }
            if (distributorAddress.DistributorPincode.Length < 6)
            {
                validDistributorAddress = false;
                sb.Append(Environment.NewLine + "Required 6 Digit Pincode");
            }
            if (validDistributorAddress == false)
                throw new InventoryException(sb.ToString());
            return validDistributorAddress;
        }

        //Validating & Adding Distributor Address
        public static bool AddDistributorAddressBL(DistributorAddress newdistributorAddress)
        {
            bool distributorAddressAdded = false;
            try
            {
                if (ValidateDistributorAddress(newdistributorAddress))
                {
                    DistributorAddressDAL distributorAddressDAL = new DistributorAddressDAL();
                    distributorAddressAdded = distributorAddressDAL.AddDistributorAddressDAL(newdistributorAddress);
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

            return distributorAddressAdded;
        }

        //Returning complete list of distributor address
        public static List<DistributorAddress> GetAllDistributorAddressBL()
        {
            List<DistributorAddress> distributorAddressList = null;
            try
            {
                DistributorAddressDAL distributorAddressDAL = new DistributorAddressDAL();
                distributorAddressList = distributorAddressDAL.GetAllDistributorAddressDAL();
            }
            catch (InventoryException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return distributorAddressList;
        }

        //Searching Distributor Address by distributor ID
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

        //Updating Distributor Address
        public static bool UpdateDistributorAddressBL(DistributorAddress updateDistributorAddress)
        {
            bool distributorAddressUpdated = false;
            try
            {
                if (ValidateDistributorAddress(updateDistributorAddress))
                {
                    DistributorAddressDAL distributorAddressDAL = new DistributorAddressDAL();
                    distributorAddressUpdated = distributorAddressDAL.UpdateDistributorAddressDAL(updateDistributorAddress);
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

            return distributorAddressUpdated;
        }

        //Deleting Distributor Address
        public static bool DeleteDistributorAddressBL(int deleteDistributorAddressID)
        {
            bool distributorAddressDeleted = false;
            try
            {
                if (deleteDistributorAddressID > 0)
                {
                    DistributorAddressDAL distributorAddressDAL = new DistributorAddressDAL();
                    distributorAddressDeleted = distributorAddressDAL.DeleteDistributorAddressDAL(deleteDistributorAddressID);
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

            return distributorAddressDeleted;
        }
    }
}