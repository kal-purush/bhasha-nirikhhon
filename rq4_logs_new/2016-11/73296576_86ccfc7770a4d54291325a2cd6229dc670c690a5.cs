using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Model.Entities;
using ErrorHandling;
using DA.Abstract;
using DA.Repository;

namespace BL.Abstract
{
    public interface IRackMasterBL
    {
        int RetrieveRackId();
        int SaveRackMasterDetails(RackMaster rack);
        int UpdateRackMasterDetails(RackMaster rack);
        int DeleteRackMasterDetails(int rackId);
        DataTable GetRackDetails();
    }
}