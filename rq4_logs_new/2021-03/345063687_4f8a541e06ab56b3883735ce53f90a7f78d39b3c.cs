using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using ClinicalDAL.EF;
namespace CASProject.Controllers
{
    public class ManageMedicineController : Controller
    {
        ClinicalDAL.UserRepository userRepo;
        ClinicalDAL.AppointmentRepository aptRepo;

        public ManageMedicineController()
        {
            userRepo = new ClinicalDAL.UserRepository();
            aptRepo = new ClinicalDAL.AppointmentRepository();
        }





        // GET: ManageMedicine
        public ActionResult Index()
        {
            return View();
        }


        public ActionResult AddMedicine()
        {
            var Med = userRepo.GetMedicine();


            return View(Med);
        }



        [HttpPost]

        public ActionResult AddMedicine(string u )
        {
            string username = Request["txtuname"];
            Medicine med = new Medicine();
            string name = Request["txtmed"];
            int qty = Convert.ToInt32(Request["txtquan"]);
            decimal tax = Convert.ToDecimal(Request["txttax"]);
            decimal dis = Convert.ToDecimal(Request["txtdis"]);
            string date = Request["txtdate"];
            decimal price = Convert.ToDecimal(Request["txtprice"]);

            decimal fprice = (price + ((tax/100)*price) - ((dis/100)*price));
            med.Name = name;
            med.Quantity = qty;
            med.Price = Convert.ToDouble(fprice);
            med.ExpDate = Convert.ToDateTime(date);


            userRepo.AddMedicine(med);
            
            return RedirectToAction("AddMedicine", "ManageMedicine", new {username=username});
        }






    }
}