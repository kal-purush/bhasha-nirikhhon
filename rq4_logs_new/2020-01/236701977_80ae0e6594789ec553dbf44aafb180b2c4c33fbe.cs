using Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Web;
using System.Web.Mvc;
using System.Net;
using System.Net.Mail;
using PagedList;

namespace LRS.Controllers
{
    public class LeaveRequestController : Controller
    {
        //public static string BaseURL = "http://192.168.2.48/LRS_API/";
        LeaveTypeInfo iLeaveTypeInfo = new LeaveTypeInfo();
        LeaveRequestInfo iLeaveRequestInfo = new LeaveRequestInfo();
        MemberInfo iMemberInfo = new MemberInfo();
        List<LeaveRequestInfo> listItems = new List<LeaveRequestInfo>();
        // GET: LeaveRequest
        [SessionTimeout]
        public ActionResult Index()
        {
            //int pageSize = 5;
            //int pageIndex = 1;
            //pageIndex = page.HasValue ? Convert.ToInt32(page) : 1;
            //IPagedList<LeaveRequestInfo> LR = null;

            using (var client = new HttpClient())
            {
                //Session["UserID"] = 6;
                if (Convert.ToBoolean(Session["IsAdmin"]) == true)
                    iLeaveRequestInfo.iMode = 110;
                else
                    iLeaveRequestInfo.iMode = 123;
                iLeaveRequestInfo.Id = Convert.ToInt32(Session["UserID"]);
                iLeaveRequestInfo.From = DateTime.Now;
                iLeaveRequestInfo.To = DateTime.Now;
                client.BaseAddress = new Uri(Utility.BaseURL);
                var response = client.PostAsJsonAsync("GetLeaveRequestDetails", iLeaveRequestInfo).Result;
                Status status = response.Content.ReadAsAsync<Status>().Result;
                if (status.Errors == null)
                {
                    DataTable dt = (DataTable)JsonConvert.DeserializeObject(status.Data.ToString(), (typeof(DataTable)));
                    if (dt.Rows.Count > 0)
                    {
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            iLeaveRequestInfo = new LeaveRequestInfo
                            {
                                Name = Convert.ToString(dt.Rows[i]["Name"]),
                                UserID = Convert.ToString(dt.Rows[i]["UserID"]),
                                From = Convert.ToDateTime(dt.Rows[i]["From"]),
                                To = Convert.ToDateTime(dt.Rows[i]["To"]),
                                Type = Convert.ToString(dt.Rows[i]["Type"]),
                                Days = Convert.ToDecimal(dt.Rows[i]["Days"]),
                                Status = Convert.ToString(dt.Rows[i]["Status"]),
                                //Message= Convert.ToString(dt.Rows[i]["Message"]),
                                Planned = Convert.ToBoolean(dt.Rows[i]["Planned"]),
                            };

                            //iLeaveRequestInfo.LRI = listItems;
                            //LR = listItems.ToPagedList(pageIndex, pageSize);
                            listItems.Add(iLeaveRequestInfo);

                        }

                    }
                }
            }
            return View(listItems);
        }

        // GET: LeaveRequest/Create
        [SessionTimeout]
        public ActionResult Create()
        {
            List<SelectListItem> items = new List<SelectListItem>();
            List<SelectListItem> itemName = new List<SelectListItem>();
            DataTable dt = new DataTable();
            using (var client = new HttpClient())
            {

                client.BaseAddress = new Uri(Utility.BaseURL);
                iLeaveTypeInfo.iMode = 113;

                var response = client.PostAsJsonAsync<LeaveTypeInfo>("GetLeavetypes", iLeaveTypeInfo).Result;
                Status status = response.Content.ReadAsAsync<Status>().Result;
                List<LeaveTypeInfo> model = (status.Text.ToLower() == "ok") ? JsonConvert.DeserializeObject<List<LeaveTypeInfo>>(status.Data.ToString()) : new List<LeaveTypeInfo>();
                foreach (var item in model) // Loop through List with foreach
                {

                    items.Add(new SelectListItem
                    {
                        Text = item.LeaveTypeName,
                        Value = Convert.ToString(item.LeaveTypeID)
                    });
                }
                ViewData["LeaveTypes"] = items;
                // iLeaveRequestInfo.Type = model;
                if (Convert.ToBoolean(Session["IsAdmin"]) == true)
                    iMemberInfo.iMode = 112;
                else
                    iMemberInfo.iMode = 124;
                iMemberInfo.ID = Convert.ToInt32(Session["UserID"]);
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
                            Value = Convert.ToString(item1.ID) + "^" + item1.EmailAddress
                        });
                    }
                }
                ViewBag.userid = Session["IsAdmin"];
                ViewData["MemberName"] = itemName;
                return View(iLeaveRequestInfo);
            }
        }

        // POST: LeaveRequest/Create
        // [ActionName("InsertLeaveInfo")]
        [HttpPost]
        [SessionTimeout]
        public ActionResult Create(LeaveRequestInfo iLeaveRequestInfo)
        {
            try
            {
                using (var client = new HttpClient())
                {
                    iLeaveRequestInfo.iMode = 107;
                    System.Globalization.CultureInfo cultureinfo = new System.Globalization.CultureInfo("en-us");
                   
                    string from = Convert.ToString(iLeaveRequestInfo.DateRange.Split('-')[0].Trim());
                    DateTime dt = DateTime.Parse(from, cultureinfo);
                    string To = Convert.ToString(iLeaveRequestInfo.DateRange.Split('-')[1].Trim());
                    DateTime dtt = DateTime.Parse(To, cultureinfo);

                    iLeaveRequestInfo.From = Convert.ToDateTime(dt);
                    iLeaveRequestInfo.To = Convert.ToDateTime(dtt);
                    iLeaveRequestInfo.UserID = iLeaveRequestInfo.UserID.Split('^')[0];
                    iLeaveRequestInfo.Name = Convert.ToString(ViewData["MemberName"]);


                    client.BaseAddress = new Uri(Utility.BaseURL);
                    var response = client.PostAsJsonAsync("SetLeaveRequestDetails", iLeaveRequestInfo).Result;
                    Status status = response.Content.ReadAsAsync<Status>().Result;
                    if (status.Errors == null)
                    {
                        //string strVerifiedMailPath = System.Web.HttpContext.Current.Server.MapPath(@"../MailFormats/AccountPassword.htm");
                        //System.IO.StreamReader sr = new System.IO.StreamReader(strVerifiedMailPath);
                        //string strVerifiedMailBody = sr.ReadToEnd();
                        //sr.Close();
                        //string strSubject = "Leave Request!";

                        //string strFrom = System.Configuration.ConfigurationManager.AppSettings["RatingMDAdmin"].ToString().Trim();
                        //string strTo = System.Configuration.ConfigurationManager.AppSettings["AdminEmail"].ToString().Trim();
                        //strVerifiedMailBody = strVerifiedMailBody.Replace("$Message$", iLeaveRequestInfo.Message);
                        //strVerifiedMailBody = strVerifiedMailBody.Replace("$Name$", iMemberInfo.FirstName);
                        //Status.SendActionEMail(strFrom, strTo, strSubject, strVerifiedMailBody, "LRS");
                        //TempData["LeaveScriptFunction"] = string.Format("fn_lr_Success();");

                        return RedirectToAction("Index");
                    }
                    else
                        return View("Index");
                }
            }
            catch (Exception ex)
            {
                return View("Index");
            }
        }

        private void BindUsers()
        {
            List<SelectListItem> itemName = new List<SelectListItem>();
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(Utility.BaseURL);
                iMemberInfo.iMode = 112;
                var response1 = client.PostAsJsonAsync<MemberInfo>("GetDetails", iMemberInfo).Result;
                Status status1 = response1.Content.ReadAsAsync<Status>().Result;

                //if (string.IsNullOrWhiteSpace(iLeaveRequestInfo.Name))
                //    ModelState.AddModelError("Message", "Please enter Name");

                if (status1.Data != null)
                {
                    List<MemberInfo> model1 = (status1.Text.ToLower() == "ok") ? JsonConvert.DeserializeObject<List<MemberInfo>>(status1.Data.ToString()) : new List<MemberInfo>();
                    // iLeaveRequestInfo.Name = model1;

                    foreach (var item1 in model1) // Loop through List with foreach
                    {

                        itemName.Add(new SelectListItem
                        {
                            Text = item1.FirstName + " " + item1.LastName,
                            Value = Convert.ToString(item1.ID) + "^" + item1.EmailAddress
                        });
                    }
                }
                ViewData["MemberName"] = itemName;
            }
        }

        private void BindLeave()
        {
            List<SelectListItem> items = new List<SelectListItem>();
            //List<SelectListItem> itemName = new List<SelectListItem>();
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(Utility.BaseURL);
                iLeaveTypeInfo.iMode = 113;
                var response = client.PostAsJsonAsync<LeaveTypeInfo>("GetLeavetypes", iLeaveTypeInfo).Result;
                Status status = response.Content.ReadAsAsync<Status>().Result;
                List<LeaveTypeInfo> model = (status.Text.ToLower() == "ok") ? JsonConvert.DeserializeObject<List<LeaveTypeInfo>>(status.Data.ToString()) : new List<LeaveTypeInfo>();
                foreach (var item in model) // Loop through List with foreach
                {
                    items.Add(new SelectListItem
                    {
                        Text = item.LeaveTypeName,
                        Value = Convert.ToString(item.LeaveTypeID)
                    });
                }
                ViewData["LeaveTypes"] = items;
            }
        }

        [SessionTimeout]
        public ActionResult GetEmail(int UserID)
        {
            try
            {
                using (var client = new HttpClient())
                {
                    iLeaveRequestInfo.iMode = 107;
                    client.BaseAddress = new Uri(Utility.BaseURL);
                    DataSet ds = new DataSet();

                    var response = client.PostAsJsonAsync("SetLeaveRequestDetails", iLeaveRequestInfo).Result;
                    Status status = response.Content.ReadAsAsync<Status>().Result;
                    if (status.Errors == null)
                    {
                        return RedirectToAction("Index");
                    }
                    else
                        return View("Index");
                }
            }
            catch
            {
                return View("Index");
            }
            // return View("Index");
        }


    }
}