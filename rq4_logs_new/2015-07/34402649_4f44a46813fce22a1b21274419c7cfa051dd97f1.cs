using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SupplyChainManagement.Util
{
    public class Values
    {
        // Table names
        public static string Item = "Item";
        public static string ItemJob = "ItemJob";
        public static string Workplace = "Workplace";
        public static string ChildToProduct = "ChildToProduct";

        // Columns
        public static string Id = "Id";
        public static string Description = "Description";
        public static string Stock = "Stock";
        public static string Value = "Value";
        public static string Product_Id = "Product_Id";
        public static string Child_Id = "Child_id";
        public static string Quantity = "Quantity";
        public static string Type = "Type";

        public static string IdleMachineCosts = "IdleMachineCosts";
        public static string JobDescription = "JobDescription";
        public static string LaborCostsFirstShift = "LaborCostsFirstShift";
        public static string LaborCostsSecondShift = "LaborCostsSecondShift";
        public static string LaborCostsThirdShift = "LaborCostsThirdShift";
        public static string LaborCostsOvertime = "LaborCostsOvertime";
        public static string ProductiveMachineCosts = "ProductiveMachineCosts";


        public static string ProductionTimePerPiece = "ProductionTimePerPiece";
        public static string SetupTime = "SetupTime";
        public static string Workplace_Id = "Workplace_Id";

        public static string DiscountQuantity = "DiscountQuantity";
        public static string OrderCosts = "OrderCosts";
        public static string ProcureLeadTime = "ProcureLeadTime";
        public static string ProcureLeadTimeDeviation = "ProcureLeadTimeDeviation";

        // Values
        public static string ProcuredItem = "ProcuredItem";
        public static string FinishedProduct = "FinishedProduct";
        public static string UnfinishedProduct = "UnfinishedProduct";
    }
}