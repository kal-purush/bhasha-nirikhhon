using System;
using System.Collections.Generic;

namespace Blockchain
{


public partial class WebRequestPostExample
{
    public string MigrateCreateBurnTransaction(WebRequestPostExample httpInstance,string DestinationName,string DestinationChainAddress, double Amount, string TokenID)
    {
        string json =httpInstance.CreateJsonRequest("migrate_createburntransaction","[\"" + DestinationName + "\"," +  "\"" + DestinationChainAddress + "\"," + Amount.ToString() + "]");
        string result = CallHttpRequest(json);
        return result;
    }

    public string MigrateConvertToExport(WebRequestPostExample httpInstance, string burnTx, string destChain)
    {
        string json = httpInstance.CreateJsonRequest("migrate_converttoexport","[" + "\"" + burnTx + "\"," + "\"" + destChain + "\"" + "]");
        string result = CallHttpRequest(json);
        return result;
    }

    public string MigrateCreateImportTransaction(WebRequestPostExample httpInstance, string burnTx, string payouts, string notarytxid_n)
    {
        string json = httpInstance.CreateJsonRequest("migrate_createimporttransaction","[" + "\"" + burnTx + "\"," + "\"" + payouts + "\"" + ((notarytxid_n!="")? "\"" + notarytxid_n + "\"" : "\"" + "\"") + "]");
        string result = CallHttpRequest(json);
        return result;
    }

    public string MigrateCompleteImportTransaction(WebRequestPostExample httpInstance, string importTx, string offset)
    {
        string json = httpInstance.CreateJsonRequest("migrate_completeimporttransaction","[" + "\"" + importTx + "\","  + ((offset!="")? "\"" + offset + "\"" : "\"" + "\"" ) + "]");
        string result = CallHttpRequest(json);
        return result;
    }

    public string MigrateCheckBurnTransactionSource(WebRequestPostExample httpInstance, string burnTxId)
    {
        string json = httpInstance.CreateJsonRequest("migrate_checkburntransactionsource","[" + "\"" + burnTxId + "\""  + "]");
        string result = CallHttpRequest(json);
        return result;
    }

    public string MigrateCreateNotaryApprovalTransaction(WebRequestPostExample httpInstance, string burnTxId, string txOutProof)
    {
        string json = httpInstance.CreateJsonRequest("migrate_createnotaryapprovaltransaction","[" + "\"" + burnTxId + "\"," + "\"" + txOutProof + "\""  + "]");
        string result = CallHttpRequest(json);
        return result;
    }

    public string SelfImport(WebRequestPostExample httpInstance, string DestAddress, double amount)
    {
        string json =httpInstance.CreateJsonRequest("selfimport","[\"" +  DestAddress + "\","  + amount.ToString() + "]");
        string result = CallHttpRequest(json);
        return result;
    }

    public string CalcMOM(WebRequestPostExample httpInstance, int height, double MoMdepth)
    {
        string json =httpInstance.CreateJsonRequest("calc_MoM","[\"" +  height.ToString() + "\","  + "\"" +  MoMdepth.ToString() + "\"" + "]");
        string result = CallHttpRequest(json);
        return result;
    }

    public string MoMoMData(WebRequestPostExample httpInstance, string symbol, int KMDHeight, int CCId)
    {
        string json =httpInstance.CreateJsonRequest("MoMoMdata","[\"" +  symbol + "\","  + "\"" +  KMDHeight.ToString() + "\"," + "\"" + CCId.ToString() + "\"" +  "]");
        string result = CallHttpRequest(json);
        return result;

    }

    public string AssetChainProof(WebRequestPostExample httpInstance, string TxId)
    {
        string json =httpInstance.CreateJsonRequest("assetchainproof","[\"" + TxId +  "\"" + "]");
        string result = CallHttpRequest(json);
        return result;
    }

    public string GetNotarizationsForBlock(WebRequestPostExample httpInstance, int height)
    {
        string json =httpInstance.CreateJsonRequest("getNotarisationsForBlock","[" + height.ToString() + "]");
        string result = CallHttpRequest(json);
        return result;
    }

    public string ScanNotarisationsDB(WebRequestPostExample httpInstance, int BlockHeight, string symbol, string BlocksLimit)
    {
        string json =httpInstance.CreateJsonRequest("scanNotarisationsDB","[" + "\"" + BlockHeight.ToString() + "\"" +  "," + "\"" + symbol + "\"," + ((BlocksLimit!="")?"\"" + BlocksLimit + "\"" : "\"" + "\"") + "]");
        string result = CallHttpRequest(json);
        return result;   
    }

    public string GetImports(WebRequestPostExample httpInstance, string BlockHash, int height)
    {
        string json =httpInstance.CreateJsonRequest("getimports","[" + ((BlockHash!="")?"\"" + BlockHash + "\"" : height.ToString()) + "]");
        string result = CallHttpRequest(json);
        return result; 
    }

    public string GetWalletBurnTransactions(WebRequestPostExample httpInstance, int count)
    {
        string json =httpInstance.CreateJsonRequest("getwalletburntransactions","[" + "\"" + count.ToString() + "\"" + "]");
        string result = CallHttpRequest(json);
        return result; 
    }
}

}