using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using Gride.Data;
using Gride.Models;

namespace Gride.Views.Shift
{
    public class ShiftsController : Controller
    {
        private readonly ApplicationDbContext _context;

        public ShiftsController(ApplicationDbContext context)
        {
            _context = context;
        }

        // GET: Shifts
        public async Task<IActionResult> Index()
        {
            return View(await _context.Shift.ToListAsync());
        }

        // GET: Shifts/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var shift = await _context.Shift
                .FirstOrDefaultAsync(m => m.ShiftID == id);
            if (shift == null)
            {
                return NotFound();
            }

            return View(shift);
        }

        // GET: Shifts/Create
        public IActionResult Create()
        {
            var shift = new Models.Shift();
            shift.ShiftSkills = new List<ShiftSkills>();
            shift.ShiftFunctions = new List<ShiftFunction>();
            PopulateAssignedFunction(shift);
            PopulateAssignedSkills(shift);
            PopulateLocationsDropDownList();
            return View();
        }

        private void PopulateSkills()
        {
            var allSkills = _context.Skill;
            var viewModel = new List<ShiftSkillsData>();
            foreach (var skill in allSkills)
            {
                viewModel.Add(new ShiftSkillsData
                {
                    SkillID = skill.SkillID,
                    Name = skill.Name,
                    Assigned = false
                });
            }
            ViewData["Skills"] = viewModel;
        }

        private void PopulateFunctions()
        {
            var allFunctions = _context.Function;
            var viewModel = new List<ShiftFunctionData>();
            foreach (var function in allFunctions)
            {
                viewModel.Add(new ShiftFunctionData
                {
                    FunctionID = function.FunctionID,
                    Name = function.Name,
                    Assigned = false
                });
            }
            ViewData["Functions"] = viewModel;
        }

        private void PopulateLocationsDropDownList(object selectedLocation = null)
        {
            var locationQuery = from l in _context.Locations
                                orderby l.Name
                                select l;
            ViewBag.LocationID = new SelectList(locationQuery.AsNoTracking(), "LocationID", "Name", selectedLocation);
        }

        // POST: Shifts/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("Start,End,LocationID,Location")] Models.Shift shift, string[] selectedSkills, string[] selectedFunctions)
        {
            if (selectedSkills != null)
            {
                shift.ShiftSkills = new List<ShiftSkills>();
                foreach (var skill in selectedSkills)
                {
                    var skillToAdd = new ShiftSkills { ShiftID = shift.ShiftID, Shift = shift, SkillID = int.Parse(skill), Skill = _context.Skill.Single(s => s.SkillID == int.Parse(skill)) };
                    shift.ShiftSkills.Add(skillToAdd);
                }
            }
            if (selectedFunctions != null)
            {
                shift.ShiftFunctions = new List<ShiftFunction>();
                foreach (var function in selectedFunctions)
                {
                    var functionToAdd = new ShiftFunction { ShiftID = shift.ShiftID, Shift = shift, FunctionID = int.Parse(function), Function = _context.Function.Single(f => f.FunctionID == int.Parse(function)) };
                    shift.ShiftFunctions.Add(functionToAdd);
                }
            }
            if (ModelState.IsValid)
            {
                _context.Add(shift);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            PopulateLocationsDropDownList(shift);
            return View(shift);
        }


        // GET: Shifts/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var shift = await _context.Shift
                .Include(s => s.ShiftFunctions).ThenInclude(s => s.Function)
                .Include(s => s.ShiftSkills).ThenInclude(s => s.Skill)
                .AsNoTracking()
                .FirstOrDefaultAsync(m => m.ShiftID == id);
            if (shift == null)
            {
                return NotFound();
            }
            PopulateLocationsDropDownList(shift.LocationID);
            PopulateAssignedSkills(shift);
            PopulateAssignedFunction(shift);
            return View(shift);
        }

        private void PopulateAssignedFunction(Models.Shift shift)
        {
            var allFunctions = _context.Function;
            var shiftFunctions = new HashSet<int>(shift.ShiftFunctions.Select(c => c.FunctionID));
            var viewModel = new List<ShiftFunctionData>();
            foreach (var function in allFunctions)
            {
                viewModel.Add(new ShiftFunctionData
                {
                    FunctionID = function.FunctionID,
                    Name = function.Name,
                    Assigned = shiftFunctions.Contains(function.FunctionID)
                });
            }
            ViewData["Functions"] = viewModel;
        }

        private void PopulateAssignedSkills(Models.Shift shift)
        {
            var allSkills = _context.Skill;
            var shiftSkills = new HashSet<int>(shift.ShiftSkills.Select(c => c.SkillID));
            var viewModel = new List<ShiftSkillsData>();
            foreach (var skill in allSkills)
            {
                viewModel.Add(new ShiftSkillsData
                {
                    SkillID = skill.SkillID,
                    Name = skill.Name,
                    Assigned = shiftSkills.Contains(skill.SkillID)
                });
            }
            ViewData["Skills"] = viewModel;
        }

        // POST: Shifts/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int? id, string[] selectedSkills, string[] selectedFunctions)
        {
            if (id == null)
            {
                return NotFound();
            }

            var shiftToUpdate = await _context.Shift
                .Include(s => s.ShiftSkills)
                    .ThenInclude(s => s.Skill)
                .Include(s => s.ShiftFunctions)
                    .ThenInclude(s => s.Function)
                .Include(s => s.Location)
                .FirstOrDefaultAsync(s => s.ShiftID == id);

            if( await TryUpdateModelAsync<Gride.Models.Shift>(shiftToUpdate, "", 
                s => s.Start, s=> s.End, s => s.LocationID))
            {
                if (String.IsNullOrWhiteSpace(shiftToUpdate.Location.Name))
                {
                    shiftToUpdate.Location = null;
                }
                UpdateShiftSkills(selectedSkills, shiftToUpdate);
                UpdateShiftFunctions(selectedFunctions, shiftToUpdate);
                try
                {
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateException /* ex */)
                {
                    //Log the error (uncomment ex variable name and write a log.)
                    ModelState.AddModelError("", "Unable to save changes. " +
                        "Try again, and if the problem persists, " +
                        "see your system administrator.");
                }
                return RedirectToAction(nameof(Index));
            }
            PopulateLocationsDropDownList(shiftToUpdate.LocationID);
            return View(shiftToUpdate);
        }

        private void UpdateShiftSkills(string[] selectedSkills, Models.Shift shiftToUpdate)
        {
            if (selectedSkills == null)
            {
                shiftToUpdate.ShiftSkills = new List<ShiftSkills>();
                return;
            }

            var selectedSkillsHS = new HashSet<string>(selectedSkills);
            if (shiftToUpdate.ShiftSkills != null)
            {
                var shiftSkills = new HashSet<int>
                (shiftToUpdate.ShiftSkills.Select(c => c.Skill.SkillID));
                foreach (var skill in _context.Skill)
                {
                    if (selectedSkillsHS.Contains(skill.SkillID.ToString()))
                    {
                        if (!shiftSkills.Contains(skill.SkillID))
                        {
                            shiftToUpdate.ShiftSkills.Add(new ShiftSkills { ShiftID = shiftToUpdate.ShiftID, SkillID = skill.SkillID });
                        }
                    }
                    else
                    {

                        if (shiftSkills.Contains(skill.SkillID))
                        {
                            ShiftSkills skillToRemove = shiftToUpdate.ShiftSkills.FirstOrDefault(i => i.SkillID == skill.SkillID);
                            _context.Remove(skillToRemove);
                        }
                    }
                }
            } else
            {
                shiftToUpdate.ShiftSkills = new List<ShiftSkills>();
                foreach (var skill in _context.Skill)
                {
                    if (selectedSkillsHS.Contains(skill.SkillID.ToString()))
                    {
                        shiftToUpdate.ShiftSkills.Add(new ShiftSkills { 
                            ShiftID = shiftToUpdate.ShiftID,
                            Shift = shiftToUpdate,
                            SkillID = skill.SkillID,
                            Skill = skill
                        });
                    }
                }
            }
            

        }

        private void UpdateShiftFunctions(string[] selectedFunctions, Models.Shift shiftToUpdate)
        {
            if (selectedFunctions == null)
            {
                shiftToUpdate.ShiftFunctions = new List<ShiftFunction>();
                return;
            }

            var selectedFunctionsHS = new HashSet<string>(selectedFunctions);
            if (shiftToUpdate.ShiftFunctions != null)
            {
                var shiftFunctions = new HashSet<int>
                (shiftToUpdate.ShiftFunctions.Select(c => c.Function.FunctionID));
                foreach (var function in _context.Function)
                {
                    if (selectedFunctionsHS.Contains(function.FunctionID.ToString()))
                    {
                        if (!shiftFunctions.Contains(function.FunctionID))
                        {
                            shiftToUpdate.ShiftFunctions.Add(new ShiftFunction { ShiftID = shiftToUpdate.ShiftID, FunctionID = function.FunctionID });
                        }
                    }
                    else
                    {

                        if (shiftFunctions.Contains(function.FunctionID))
                        {
                            ShiftFunction functionToRemove = shiftToUpdate.ShiftFunctions.FirstOrDefault(i => i.FunctionID == function.FunctionID);
                            _context.Remove(functionToRemove);
                        }
                    }
                }
            } else
            {
                shiftToUpdate.ShiftFunctions = new List<ShiftFunction>();
                foreach (var function in _context.Function)
                {
                    if (selectedFunctionsHS.Contains(function.FunctionID.ToString()))
                    {
                        shiftToUpdate.ShiftFunctions.Add(new ShiftFunction
                        {
                            ShiftID = shiftToUpdate.ShiftID,
                            Shift = shiftToUpdate,
                            FunctionID = function.FunctionID,
                            Function = function
                        });
                    }
                }
            }

        }

        // GET: Shifts/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var shift = await _context.Shift
                .FirstOrDefaultAsync(m => m.ShiftID == id);
            if (shift == null)
            {
                return NotFound();
            }

            return View(shift);
        }

        // POST: Shifts/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(ulong id)
        {
            var shift = await _context.Shift.FindAsync(id);
            _context.Shift.Remove(shift);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool ShiftExists(int id)
        {
            return _context.Shift.Any(e => e.ShiftID == id);
        }
    }
}