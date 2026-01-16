using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BLL.Services.Interfaces;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using OrganisationArchive.DAL.Models;

namespace OrganisationArchive.Controllers
{
    public class OrganizationController : Controller
    {
        private IOrganizationService _organizationService;

        public OrganizationController(IOrganizationService organizationService)
        {
            _organizationService = organizationService;
        }
        // GET: Organization
        public ActionResult Organizations()
        {
            var organizations = _organizationService.GetOrganizations();
            return View(organizations);
        }

        // GET: Organization/Details/5
        public ActionResult Details(int id)
        {
            return View();
        }

        // GET: Organization/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: Organization/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create(Organization organization)
        {
            try
            {
                // TODO: Add insert logic here
                _organizationService.AddOrganization(organization);
                return RedirectToAction("Organizations");
            }
            catch
            {
                return View();
            }
        }

        // GET: Organization/Edit/5
        public ActionResult Edit(int id)
        {
            return View();
        }

        // POST: Organization/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Edit(int id, Organization organization)
        {
            try
            {
                // TODO: Add update logic here
                _organizationService.UpdateOrganization(organization);
                return RedirectToAction("Organizations");
            }
            catch
            {
                return View();
            }
        }

        // GET: Organization/Delete/5
        public ActionResult Delete(int id)
        {
            return View();
        }

        // POST: Organization/Delete/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Delete(int id, Organization organization)
        {
            try
            {
                // TODO: Add delete logic here
                _organizationService.DeleteOrganization(organization);
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }
    }
}