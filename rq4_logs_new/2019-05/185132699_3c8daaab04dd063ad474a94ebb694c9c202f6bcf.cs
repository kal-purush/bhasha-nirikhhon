using Dao.Utils;
using Model;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dao
{
    public class SupplyDao
    {
        public List<Supply> querySupplyByName(String supplyName) {
            List<Supply> list = new List<Supply>();
            String sql = "SELECT * FROM tb_supply WHERE supply_name = @supply_name";
            SqlCommand cmd = DbUtil.getCommand(sql);
            Supply supply = new Supply();
            SqlDataReader sdr = cmd.ExecuteReader();
            if (sdr.HasRows) {
                while (sdr.NextResult())
                {
                    supply.Id = sdr.GetInt32(0);
                    supply.SupplyName = sdr.GetString(1);
                    supply.SupplyDesc = sdr.GetString(2);
                    supply.SupplyCategory.Id = sdr.GetInt32(3);
                    supply.Priority = sdr.GetInt32(4);
                    supply.User.Id = sdr.GetInt32(5);
                    supply.CreateTime = sdr.GetDateTime(6);
                    supply.ModifyTime = sdr.GetDateTime(7);
                    supply.SupplyStatus = sdr.GetInt32(8);
                    list.Add(supply);
                }
            }
            return list;
        }
        public List<Supply> queryAllSupply()
        {
            List<Supply> list = new List<Supply>();
            String sql = "SELECT * FROM tb_supply";
            SqlCommand cmd = DbUtil.getCommand(sql);
            
            SqlDataReader sdr = cmd.ExecuteReader();
            if (sdr.HasRows)
            {
                while (sdr.Read())
                {
                    Supply supply = new Supply();
                    supply.Id = sdr.GetInt32(0);
                    supply.SupplyName = sdr.GetString(1);
                    supply.SupplyDesc = sdr.GetString(2);
                    supply.SupplyCategory.Id = sdr.GetInt32(3);
                    supply.Priority = sdr.GetInt32(4);
                    supply.User.Id = sdr.GetInt32(5);
                    //supply.CreateTime = sdr.GetDateTime(6);
                    //supply.ModifyTime = sdr.GetDateTime(7);
                    supply.SupplyStatus = sdr.GetInt32(8);
                    //sdr.NextResult();
                    list.Add(supply);
                }
            }
            return list;
        }
    }
}