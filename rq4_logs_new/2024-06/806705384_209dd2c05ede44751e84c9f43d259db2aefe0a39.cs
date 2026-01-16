using DTO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DAL
{
    public class FoodOnComDAL
    {
        private static FoodOnComDAL instance;

        public static FoodOnComDAL Instance
        {
            get
            {
                if (instance == null)
                {
                    instance = new FoodOnComDAL();
                }
                return FoodOnComDAL.instance;
            }
            private set { FoodOnComDAL.instance = value; }
        }

        private FoodOnComDAL() { }

        public List<FoodOnCom> GetFoodDetail(byte comid)
        {
            List<FoodOnCom> fl = new List<FoodOnCom>();

            string query = "GetFoodDetailsByComputerID @ComputerID";
            DataTable dt = Database.Instance.ExecuteQuery(query, new object[] { comid });

            foreach (DataRow dr in dt.Rows)
            {
                FoodOnCom f = new FoodOnCom(dr);
                fl.Add(f);
            }

            return fl;
        }
    }
}