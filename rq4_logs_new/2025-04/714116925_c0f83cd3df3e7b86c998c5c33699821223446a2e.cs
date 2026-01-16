using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Budget
{
    public enum AssetType
    {
        BankAndCash,
        Investments,
        Both
    }

    public class AssetTypeClass
    {
        public AssetType AssetType => _AssetType;
        AssetType _AssetType;

        public string Label => _Label;
        string _Label;

        public AssetTypeClass(AssetType assetType, string label) 
        {
            _AssetType = assetType;
            _Label = label;
        }

        public static AssetTypeClass[] List => new AssetTypeClass[]
            {
            new AssetTypeClass(AssetType.BankAndCash, "Bank/Cash"),
            new AssetTypeClass(AssetType.Investments, "Investments"),
            new AssetTypeClass(AssetType.Both, "Both")
            };
    }
}