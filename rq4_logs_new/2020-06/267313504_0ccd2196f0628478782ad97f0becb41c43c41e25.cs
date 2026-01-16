using FAMS_AngularApi.Models.BrokerMaster;
using FAMS_AngularApi.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace FAMS_AngularApi.Controllers
{
    public class BrokerMasterController : ApiController
    {
        BrokerMasterDataAccess Obj = new BrokerMasterDataAccess();
        [HttpPost]
        [Route("api/BrokerMaster/BindGrid")]
        public Dictionary<string, object> BindGridApi(CommonFields Data)
        {
            return Obj.BindGrid(Data);
        }
        [HttpPost]
        [Route("api/BrokerMaster/SaveData")]
        public Dictionary<string, object> SaveDataApi(JsonData Data)
        {
            return Obj.SaveData(Data);
        }
    }
}