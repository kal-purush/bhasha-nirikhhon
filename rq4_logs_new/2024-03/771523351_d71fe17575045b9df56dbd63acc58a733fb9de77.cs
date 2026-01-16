using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using SomerenModel;

namespace SomerenDAL
{
    public class DrinkDao : BaseDao
    {
        public List<Drinks> GetAllDrinks()
        {
            string query = "SELECT DrinkID, DrinkName, DrinkVAT, DrinkType, DrinkPrice, DrinkStock FROM dbo.DRINKS";
            SqlParameter[] sqlParameters = new SqlParameter[0];
            return ReadTables(ExecuteSelectQuery(query, sqlParameters));
        }

        private List<Drinks> ReadTables(DataTable dataTable)
        {
            List<Drinks> drinks = new List<Drinks>();

            foreach (DataRow dr in dataTable.Rows)
            {
                Drinks drink = new Drinks()
                {

                    DrinkID = (int)dr["DrinkID"],
                    DrinkName = dr["DrinkName"].ToString(),
                    DrinkVAT = (decimal)dr["DrinkVAT"],
                    DrinkType = dr["DrinkType"].ToString(),
                    DrinkPrice = (decimal)dr["DrinkPrice"],
                    DrinkStock = (int)dr["DrinkStock"]

            };
                drinks.Add(drink);
            }
            return drinks;
        }
    }
}