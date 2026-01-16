using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EZ.DB;

namespace KincoP2L
{
    public static class LocatorManager
    {
        public static decimal NewID()
        {
          return  DBAccessor.FromDefaultDb().ExecuteScalar<decimal>("SELECT SEQ_ID.NEXTVAL FROM DUAL ");
        }

        public static decimal GetLocatorID(string locatorCode)
        {
            string cmd = " SELECT ID FROM IMS_LOCATOR WHERE CODE=:CODE";
            return DBAccessor.FromDefaultDb().ExecuteScalarByKeyValuePairs<decimal>(
                cmd,
                new KeyValuePair<string, object>("CODE", locatorCode));
        }

        public static RackDataSet.IMS_LOCATORRow GetLocatorRow(string locatorCode)
        {
            RackDataSet.IMS_LOCATORDataTable tb = DBAccessor.FromDefaultDb().GetDataTableByKeyValuePairs<RackDataSet.IMS_LOCATORDataTable>(
                "SELECT * FROM IMS_LOCATOR WHERE CODE=:CODE",
                new KeyValuePair<string, object>("CODE", locatorCode));
            return tb.FirstOrDefault();
        }

        public static bool RackAddressExists(decimal rackAddress)
        {
            string cmd = @" SELECT COUNT('*')
    FROM IMS_INTELLIGENT_RACK_CELL_INFO  A
    WHERE A.RACK_ADDRESS IN (:ADDRESS, :ADDRESS+1)";
            decimal count = DBAccessor.FromDefaultDb().ExecuteScalarByKeyValuePairs<decimal>(
                 cmd,
                 new KeyValuePair<string, object>("ADDRESS", rackAddress));
            if (count > 0) 
                return true; 
            else 
                return false;
        }

        public static decimal SuggestRackAddress()
        {
            string cmd = @"SELECT NVL(RACK_ADDRESS,1)
  FROM (
SELECT MAX (A.RACK_ADDRESS) + 1 RACK_ADDRESS
  FROM IMS_INTELLIGENT_RACK_CELL_INFO A )";
            decimal address = DBAccessor.FromDefaultDb().ExecuteScalar<decimal>(cmd);
            if (address % 2 == 0)
                return address + 1;
            else
                return address;
        }

        public static decimal GetRackAddress(string rackLocatorCode)
        {
            string cmd = @"SELECT MIN (A.RACK_ADDRESS)
  FROM IMS_INTELLIGENT_RACK_CELL_INFO A, IMS_LOCATOR L
 WHERE A.RACK_LOCATOR_ID = L.ID AND L.CODE = :CODE ";
            return DBAccessor.FromDefaultDb().ExecuteScalarByKeyValuePairs<decimal>(cmd, new KeyValuePair<string, object>("CODE", rackLocatorCode));
        }

        public static void SaveData(System.Data.DataTable table)
        {
            DBAccessor.FromDefaultDb().UpdateDataTable(table, table.TableName);
        }

    }
}