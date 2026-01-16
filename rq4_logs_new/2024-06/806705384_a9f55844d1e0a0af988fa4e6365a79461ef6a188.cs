using DTO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DAL
{
    public class MaintainanceDAL
    {
        private static MaintainanceDAL instance;

        public static MaintainanceDAL Instance
        {
            get
            {
                if (instance == null)
                {
                    instance = new MaintainanceDAL();
                }
                return MaintainanceDAL.instance;
            }
            private set { MaintainanceDAL.instance = value; }
        }

        private MaintainanceDAL() { }

        public void StartMaintainance(byte comId)
        {
            Database.Instance.ExecuteNonQuery("ProcBillingINIT @Billingtype", new object[] { 0 });
            Database.Instance.ExecuteNonQuery("ProcMaintainanceINIT @ComputerID ", new object[] { comId });

        }
        public void AddMaintainanceDetail(int maintainid, string component, string mota)
        {
            Database.Instance.ExecuteNonQuery("AddMaintainanceDetail @maintainid , @component , @mota", new object[] { maintainid, component, mota });

        }

        public List<Maintainance> GetListMaintainance(byte comid)
        {
            List<Maintainance> mlist = new List<Maintainance>();
            string query = "SELECT m.billingid, md.ComponentName, md.Description, m.maintainanceid FROM Maintainance m JOIN MaintainanceDetail md ON m.MaintainanceID = md.MaintainanceID WHERE m.cost is null and m.ComputerID = " + comid;
            DataTable dt = Database.Instance.ExecuteQuery(query);

            foreach (DataRow dr in dt.Rows)
            {
                Maintainance m = new Maintainance(dr);
                mlist.Add(m);
            }

            return mlist;
        }
        public int GetMaintainanceIDMAX()
        {
            try
            {
                return (int)Database.Instance.ExecuteScalar("SELECT MAX(MaintainanceID) from Maintainance");
            }
            catch
            {
                return 1;
            }
        }

        public int GetUnCheckOutMaintainance(byte comid)
        {
            DataTable dt = Database.Instance.ExecuteQuery("GetUnCheckOutMaintainance @comid", new object[] { comid });
            if (dt.Rows.Count > 0)
            {
                Maintainance m = new Maintainance(dt.Rows[0]);
                return m.MaintainId;
            }
            return -1;
        }
    }
}