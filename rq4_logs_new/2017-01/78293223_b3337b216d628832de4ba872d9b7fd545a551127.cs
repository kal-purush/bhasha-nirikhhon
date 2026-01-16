using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace CIS467_AMP.Areas.Shared.Model
{
    /// <summary>
    /// List of currently installed assets
    /// 
    /// AssetInventoryID - id of particular entry
    /// AssetName - Name of specific asset
    /// VariantID - specific variant of asset type
    /// LocationID - id of specific location asset is related to or installed in
    /// Active - is asset currently active or has it been removed/replaced/ (Currently active = true)
    /// </summary>
    public class AssetInventory
    {
        public int AssetInventoryID { get; set; }
        public string AssetName { get; set; }
        public int VariantID { get; set; }
        public int MapID { get; set; }
        public bool Active { get; set; }
    }
}