using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using FAMS_AngularApi.Models.LinkSetup;


namespace FAMS_AngularApi.Controllers
{
    public class LinkSetupController : ApiController
    {
        DataAccessLayer ObjDAL = new DataAccessLayer();
        [HttpPost]
        [Route("api/LinkSetup/GetLinks")]
        public Dictionary<string, object> getLinksUserWise([FromBody] JsonLink LinkData)
        {
            return ObjDAL.GetLinks(LinkData);
        }
    }
}