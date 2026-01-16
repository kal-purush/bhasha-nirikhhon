using FAMS_AngularApi.Models;
using FAMS_AngularApi.Models.MyNotes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace FAMS_AngularApi.Controllers
{
    public class MyNotesController : ApiController
    {
        DataAccessLayer ObjDAL = new DataAccessLayer();
        [HttpPost]
        [Route("api/MyNotes/BindGrid")]
        public Dictionary<string, object> BindGridApi(CommonFields Data)
        {
            return ObjDAL.BindGrid(Data);
        }
        //[HttpPost]
        //[Route("api/MyNotes/ReadMessage")]
        //public Dictionary<string, object> ReadMessageApi(CommonFields Data)
        //{
        //    return ObjDAL.ReadMessage(Data);
        //}
    }
}