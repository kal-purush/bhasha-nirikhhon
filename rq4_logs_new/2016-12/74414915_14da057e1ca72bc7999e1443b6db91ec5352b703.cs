using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BizWorks
{
    public class Asset
    {
        private int assetID;
        private string assetName;
        private float assetValue;
        private float assetQuantity;
        private string category;
        private float totalValue;
   
        
        private static List<Asset> AssetList = new List<Asset>();
     

        //constructor
        public Asset(string assetName, float assetValue, float assetQuantity, string category){
            assetName = assetName;
            assetValue = assetValue;
            assetQuantity = assetQuantity;
            category = category;
            assetID = assetList.size();
            totalValue = assetValue * assetQuantity;
        }
        
        public static update(assetID, isCredit, transactionAmount){
            // update asset amount
        }
        
        public int assetID
        {
            get { return assetID; }
            set { assetID = value; }
        }
        
        public string assetName
        {
            get { return assetName; }
            set {assetName = value; }
        }
  
        public string category
        {
            get { return category; }
            set { category = value; }
        }
     
        public float assetValue
        {
            get { return assetValue; }
            set { assetValue = value; }
        }
        
        public float assetQuantity
        {
            get {return assetQuantity; }
            set { assetQuantity = value; }
        }
          
    }
}