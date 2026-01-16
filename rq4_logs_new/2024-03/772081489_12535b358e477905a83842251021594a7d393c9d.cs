using AlanJuden.MvcReportViewer;
using Microsoft.AspNetCore.Mvc;
using System.Net;

namespace Examination.Controllers
{
    public class ReportController : AlanJuden.MvcReportViewer.ReportController
    {
        protected override ICredentials NetworkCredentials
        {
            get
            {
                return new System.Net.NetworkCredential("username", "pssword");
            }
           
        }

        protected override string ReportServerUrl 
        {
            get
            {
                return "http://DESKTOP-KKEBIIH/ReportServer/ReportExection2005.asmx";
            }

        }
        public  IActionResult ProcessReport()
        {
            var model = this.GetReportViewerModel(Request);
            model.ReportPath = "/Report/GetExam";
            return RedirectToAction("ExamReport", model);
        }
        public IActionResult ExamReport( )
        {
            var model = new ReportViewerModel();
            return View(model);

        }


    }

}