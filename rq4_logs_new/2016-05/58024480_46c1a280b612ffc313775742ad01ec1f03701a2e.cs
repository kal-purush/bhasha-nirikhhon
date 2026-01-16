using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Mvc;
using GrabCarMVC.Models;

namespace GrabCarMVC.Controllers
{
    public class DriverController : Controller
    {
        private deshConnection db = new deshConnection();

        // GET: /Driver/
        public ActionResult Index()
        {
            return View(db.user_driver.ToList());
        }

        // GET: /Driver/Details/5
        public ActionResult Details(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            user_driver user_driver = db.user_driver.Find(id);
            if (user_driver == null)
            {
                return HttpNotFound();
            }
            return View(user_driver);
        }

        // GET: /Driver/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: /Driver/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create([Bind(Include="id,first_name,last_name,fullname,phone,email,picture,bio,address,city,state,country,zipcode,dog_id,password,token,token_expiry,device_token,device_type,login_by,social_unique_id,created_at,updated_at,latitude,longitude,debt,deleted_at,rate,rate_count,promo_count,is_referee,user_type,gcm_reg_id,current_location_road,car_no,car_model_no,manufacture_year,car_type,gender,age,present_address,license_image,nid_image,brtc_image,insurance_image,status")] user_driver user_driver)
        {
            if (ModelState.IsValid)
            {
                db.user_driver.Add(user_driver);
                db.SaveChanges();
                return RedirectToAction("Index");
            }

            return View(user_driver);
        }

        // GET: /Driver/Edit/5
        public ActionResult Edit(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            user_driver user_driver = db.user_driver.Find(id);
            if (user_driver == null)
            {
                return HttpNotFound();
            }
            return View(user_driver);
        }

        // POST: /Driver/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Edit([Bind(Include="id,first_name,last_name,fullname,phone,email,picture,bio,address,city,state,country,zipcode,dog_id,password,token,token_expiry,device_token,device_type,login_by,social_unique_id,created_at,updated_at,latitude,longitude,debt,deleted_at,rate,rate_count,promo_count,is_referee,user_type,gcm_reg_id,current_location_road,car_no,car_model_no,manufacture_year,car_type,gender,age,present_address,license_image,nid_image,brtc_image,insurance_image,status")] user_driver user_driver)
        {
            if (ModelState.IsValid)
            {
                db.Entry(user_driver).State = EntityState.Modified;
                db.SaveChanges();
                return RedirectToAction("Index");
            }
            return View(user_driver);
        }

        // GET: /Driver/Delete/5
        public ActionResult Delete(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            user_driver user_driver = db.user_driver.Find(id);
            if (user_driver == null)
            {
                return HttpNotFound();
            }
            return View(user_driver);
        }

        // POST: /Driver/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public ActionResult DeleteConfirmed(int id)
        {
            user_driver user_driver = db.user_driver.Find(id);
            db.user_driver.Remove(user_driver);
            db.SaveChanges();
            return RedirectToAction("Index");
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                db.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}