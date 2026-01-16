using Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Web;
using System.Web.Mvc;

namespace LRS.Controllers
{
    public class COffDetailsController : Controller
    {
        MemberInfo iMemberInfo = new MemberInfo();
        LeaveRequestInfo iLeaveRequestInfo = new LeaveRequestInfo();
        //public static string BaseURL = "http://192.168.2.48/LRS_API/";
        // GET: COffDetails

        [SessionTimeout]
        public ActionResult Index()
        {
            //if (Convert.ToString(Session["Email"]) == "")
            //{
            //    return RedirectToAction("Index", "Login");
            //}
            using (var client = new HttpClient())
            {
                List<SelectListItem> itemName = new List<SelectListItem>();
                client.BaseAddress = new Uri(Utility.BaseURL);
                iMemberInfo.iMode = 112;
                var response1 = client.PostAsJsonAsync<MemberInfo>("GetDetails", iMemberInfo).Result;
                Status status1 = response1.Content.ReadAsAsync<Status>().Result;
                if (status1.Data != null)
                {
                    List<MemberInfo> model1 = (status1.Text.ToLower() == "ok") ? JsonConvert.DeserializeObject<List<MemberInfo>>(status1.Data.ToString()) : new List<MemberInfo>();
                    // iLeaveRequestInfo.Name = model1;

                    foreach (var item1 in model1) // Loop through List with foreach
                    {
                        itemName.Add(new SelectListItem
                        {
                            Text = item1.FirstName + " " + item1.LastName,
                            Value = Convert.ToString(item1.ID)
                        });
                    }
                    itemName.Insert(0, new SelectListItem()
                    {
                        Value = "0",
                        Text = "-- Please select user --"
                    });
                    ViewData["MemberName"] = itemName;
                }
                else
                {
                    itemName.Add(new SelectListItem
                    {
                        Text = "",
                        Value = ""
                    });
                    ViewData["MemberName"] = itemName;
                }
                return View();
            }
        }

        [HttpPost]
        [SessionTimeout]
        public ActionResult Create(COffInfo iCOffInfo)
        {
            if (string.IsNullOrWhiteSpace(Convert.ToString(iCOffInfo.COffDay)))
                ModelState.AddModelError("COffDay", "Please enter day");

            if (string.IsNullOrWhiteSpace(Convert.ToString(iCOffInfo.COffDateRange)))
                ModelState.AddModelError("COffDate", "Please enter date");

            if (Convert.ToString(iCOffInfo.ID) == "0")
                ModelState.AddModelError("ID", "Please select user");
            if (!ModelState.IsValid)
            {
                //return View("Index");
                return RedirectToAction("Index");
            }

            try
            {
                using (var client = new HttpClient())
                {

                    iCOffInfo.iMode = 114;
                    //iCOffInfo.ID = iCOffInfo.ID;

                    System.Globalization.CultureInfo cultureinfo = new System.Globalization.CultureInfo("en-us");

                    string from = Convert.ToString(iCOffInfo.COffDateRange.Split('-')[0]);
                    DateTime dt = DateTime.Parse(from, cultureinfo);

                    string To = Convert.ToString(iCOffInfo.COffDateRange.Split('-')[1]);
                    DateTime dtt = DateTime.Parse(To, cultureinfo);

                    iCOffInfo.From = Convert.ToDateTime(dt);
                    iCOffInfo.To = Convert.ToDateTime(dtt);
                    iCOffInfo.COffDate = DateTime.Now;
                    client.BaseAddress = new Uri(Utility.BaseURL);
                    var response = client.PostAsJsonAsync("SetCOffDetails", iCOffInfo).Result;
                    Status status = response.Content.ReadAsAsync<Status>().Result;
                    if (status.Errors == null)
                    {
                        TempData["JavaScriptFunction"] = string.Format("fnSuccess('0');");
                        return RedirectToAction("Index");
                    }
                    else
                    {
                        TempData["JavaScriptFunction"] = string.Format("fnSuccess('1');");
                       
                    }
                    return View("Index");
                }
            }
            catch (Exception ex)
            {
                return View("Index");
            }

        }

       
    }
}