using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DAL;
using Entity;

namespace BLL
{
    public class BC_Orders
    {
        DC_Orders dal = new DC_Orders();
        public MC_Orders GetModelByVedioID(int VedioID, int UID)
        {
            return dal.GetModelByVedioID(VedioID,UID);
        }
    }
}