using Project1.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace Project1.Controllers
{
    public class ValidationController : Controller
    {
        private AlorOvijatriEntities db = new AlorOvijatriEntities();

        [HttpPost]
        public JsonResult IsPaymentIDExist(string PaymentID)
        {
            var user = db.tbl_Deposit.Find(PaymentID);
            return Json(user == null);
        }

        [HttpPost]
        public JsonResult IsUserIDExist(string UserID)
        {
            var user = db.tbl_UserInfo.Find(UserID);
            return Json(user == null);
        }
	}
}