using IdeasRepository.BL.Interfaces;
using IdeasRepository.BL.Providers;
using IdeasRepository.DAL.Entities;
using IdeasRepository.Web.Models.Records;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace IdeasRepository.Web.Controllers
{
    public class RecordController : Controller
    {
        private IRecordsProvider _provider;

        public RecordController()
        {
            _provider = new RecordsProvider();
        }

        public ActionResult List()
        {
            var records = _provider.GetAllRecords();
            var recordsViewModel = new List<RecordViewModel>();

            if (records != null)
            {
                foreach (var record in records)
                {
                    recordsViewModel.Add(new RecordViewModel
                    {
                        Id = record.Id,
                        Author = record.Author,
                        CreationDate = record.CreationDate.ToString(),
                        RecordType = record.RecordType.Name,
                        TextBody = record.TextBody
                    });
                } 
            }

            return View(recordsViewModel);
        }

        public ActionResult Create()
        {
            return View();
        }

        [HttpPost]
        public ActionResult Create(RecordViewModel record)
        {
            if (ModelState.IsValid)
            {
                var recordDataModel = new Record
                {
                    Id = Guid.NewGuid().ToString(),
                    Author = HttpContext.User.Identity.Name,
                    CreationDate = DateTime.Now,
                    TextBody = record.TextBody,
                    RecordTypeId = _provider.GetRecordTypeByName(record.RecordType).Id
                };

                _provider.AddRecord(recordDataModel);

                return RedirectToAction("List");
            }
            else
            {
                return View(record);
            }
        }

        public ActionResult Edit(string id)
        {
            var record = _provider.GetRecord(id);
            var recordViewModel = new RecordViewModel
            {
                Id = record.Id,
                Author = record.Author,
                CreationDate = record.CreationDate.ToString(),
                RecordType = record.RecordType.Name,
                TextBody = record.TextBody
            };

            return View(recordViewModel);
        }

        [HttpPost]
        public ActionResult Edit(RecordViewModel record)
        {
            if (ModelState.IsValid)
            {
                var recordDataModel = new Record
                {
                    Id = record.Id,
                    TextBody = record.TextBody,
                    RecordTypeId = _provider.GetRecordTypeByName(record.RecordType).Id
                };

                _provider.UpdateRecord(recordDataModel);

                return RedirectToAction("List");
            }
            else
            {
                return View(record);
            }
        }

        [HttpPost]
        public ActionResult Remove(string id)
        {
            var recordDataModel = new Record
            {
                Id = id
            };

            _provider.RemoveRecord(recordDataModel);

            return View("List");
        }

        protected override void Dispose(bool disposing)
        {
            _provider.Dispose();
            base.Dispose(disposing);
        }
    }
}