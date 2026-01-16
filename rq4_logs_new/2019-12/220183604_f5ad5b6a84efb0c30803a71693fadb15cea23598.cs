using CRM.Models;
using CRM.Services;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CRM.Controllers
{
    public class AttendanceController : Controller
    {
        private readonly AttendanceService _attendanceService;

        public AttendanceController(AttendanceService attendanceService)
        {
            _attendanceService = attendanceService;
        }

        public async Task<ActionResult> Index()
        {
            var attendaces = await _attendanceService.GetAllAttendance();

            return View(attendaces);
        }

        // GET: Leve/Details/5
        public async Task<ActionResult> Details(int id)
        {
            var attendace = await _attendanceService.GetById(id);
            return View(attendace);
        }

        // GET: Leve/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: Leve/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Create(Attendance attendace)
        {
            if (ModelState.IsValid)
            {
                await _attendanceService.CreateAttendance(attendace);
                return RedirectToAction(nameof(Index));
            }
            return View(attendace);
        }

        // GET: Leve/Edit/5
        public async Task<ActionResult> Edit(int id)
        {
            var attendace = await _attendanceService.GetById(id);
            return View(attendace);
        }

        // POST: Leve/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Edit(Attendance attendance)
        {
            try
            {
                await _attendanceService.EditAttendance(attendance);
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
            var attendace = await _attendanceService.GetById(id);
            return View(attendace);
        }

        // POST: Leve/Delete/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Delete(Attendance attendance)
        {
            try
            {
                await _attendanceService.DeleteAttendance(attendance);
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return RedirectToAction(nameof(Index));
            }
        }
    }
}