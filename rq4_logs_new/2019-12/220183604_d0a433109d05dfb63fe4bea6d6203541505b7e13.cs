using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CRM.Models;
using CRM.Services;
using Microsoft.AspNetCore.Mvc;

namespace CRM.Controllers
{
    public class TimeTableController : Controller
    {
        private readonly TimeTableService _timeTableService;

        public TimeTableController(TimeTableService timeTableService)
        {
            _timeTableService = timeTableService;
        }

        // GET: Leve
        public async Task<ActionResult> Index()
        {
            var timeTables = await _timeTableService.GetAllTimeTable();

            return View(timeTables);
        }

        // GET: Leve/Details/5
        public async Task<ActionResult> Details(int id)
        {
            var timeTable = await _timeTableService.GetTimeTableById(id);
            return View(timeTable);
        }

        // GET: Leve/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: Leve/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Create(TimeTable timeTable)
        {
            if (ModelState.IsValid)
            {
                await _timeTableService.CreateTimeTable(timeTable);
                return RedirectToAction(nameof(Index));
            }
            return View(timeTable);
        }

        // GET: Leve/Edit/5
        public async Task<ActionResult> Edit(int id)
        {
            var timeTable = await _timeTableService.GetTimeTableById(id);
            return View(timeTable);
        }

        // POST: Leve/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Edit(TimeTable timeTable)
        {
            try
            {
                await _timeTableService.EditTimeTable(timeTable);
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return RedirectToAction(nameof(Index));
            }
        }

        // GET: Leve/Delete/5
        public async Task<ActionResult> Delete(int id)
        {
            var timeTable = await _timeTableService.GetTimeTableById(id);
            return View(timeTable);
        }

        // POST: Leve/Delete/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Delete(TimeTable timeTable)
        {
            try
            {
                await _timeTableService.DeleteTimeTable(timeTable);
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return RedirectToAction(nameof(Index));
            }
        }
    }
}