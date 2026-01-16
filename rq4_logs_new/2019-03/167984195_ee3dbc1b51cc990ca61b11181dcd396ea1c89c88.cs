using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using TRPR.Data;
using TRPR.Models;
using TRPR.Utilities;

namespace TRPR.Controllers
{
    public class PaperInfoTestController : Controller
    {
        private readonly TRPRContext _context;

        public PaperInfoTestController(TRPRContext context)
        {
            _context = context;
        }

        // GET: PaperInfoTest
        public async Task<IActionResult> Index(string SearchString, int? PaperTypeID, int? StatusID, int? KeywordID, int? page, string sortDirection, string actionButton, string sortField = "Title")
        {
            PopulateDropDownLists();
            ViewData["KeywordID"] = new SelectList(_context.Keywords.OrderBy(p => p.KeyWord), "ID", "KeyWord");
            ViewData["Filtering"] = "";

            var papers = from p in _context.PaperInfos
                .Include(p => p.Status)
                .Include(p => p.Files)
                .Include(p => p.AuthoredPapers)
                .ThenInclude(pc => pc.Researcher)
                .Include(p => p.PaperKeywords)
                .ThenInclude(pc => pc.Keyword)
                         select p;

            int pageSize = 10;//Change as required
            var pagedData = await PaginatedList<PaperInfo>.CreateAsync(papers.AsNoTracking(), page ?? 1, pageSize);

            //Add as many filters as needed
            if (PaperTypeID.HasValue)
            {
                papers = papers.Where(p => p.PaperTypeID == PaperTypeID);
                ViewData["Filtering"] = " in";
            }
            if (StatusID.HasValue)
            {
                papers = papers.Where(p => p.StatusID == StatusID);
                ViewData["Filtering"] = " in";
            }
            if (KeywordID.HasValue)
            {
                papers = papers.Where(p => p.PaperKeywords.Any(c => c.KeywordID == KeywordID));
                ViewData["Filtering"] = " in";
            }

            if (!String.IsNullOrEmpty(SearchString))
            {
                papers = papers.Where(p => p.PaperTitle.ToUpper().Contains(SearchString.ToUpper()));
                ViewData["Filtering"] = " in";
            }

            //Before we sort, see if we have called for a change of filtering or sorting
            if (!String.IsNullOrEmpty(actionButton)) //Form Submitted so lets sort!
            {
                page = 1;//Reset page to start
                if (actionButton != "Filter")//Change of sort is requested
                {
                    if (actionButton == sortField) //Reverse order on same field
                    {
                        sortDirection = String.IsNullOrEmpty(sortDirection) ? "desc" : "";
                    }
                    sortField = actionButton;//Sort by the button clicked
                }
            }
            //Now we know which field and direction to sort by, but a Switch is hard to use for 2 criteria
            //so we will use an if() structure instead.
            if (sortField == "Status")//Sorting by Patient Name
            {
                if (String.IsNullOrEmpty(sortDirection))
                {
                    papers = papers
                        .OrderBy(p => p.Status.StatName);
                }
                else
                {
                    papers = papers
                       .OrderByDescending(p => p.Status.StatName);
                }
            }
            else if (sortField == "Paper Type")
            {
                if (String.IsNullOrEmpty(sortDirection))
                {
                    papers = papers
                        .OrderBy(p => p.PaperType.Name);
                }
                else
                {
                    papers = papers
                        .OrderByDescending(p => p.PaperType.Name);
                }
            }
            else if (sortField == "Length")
            {
                if (String.IsNullOrEmpty(sortDirection))
                {
                    papers = papers
                        .OrderBy(p => p.PaperLength);
                }
                else
                {
                    papers = papers
                        .OrderByDescending(p => p.PaperLength);
                }
            }
            else //Sorting by Title - the default sort order
            {
                if (String.IsNullOrEmpty(sortDirection))
                {
                    papers = papers
                        .OrderBy(p => p.PaperTitle);
                }
                else
                {
                    papers = papers
                       .OrderByDescending(p => p.PaperTitle);
                }
            }
            //Set sort for next time
            ViewData["sortField"] = sortField;
            ViewData["sortDirection"] = sortDirection;

            return View(pagedData);
        }

        // GET: PaperInfoTest/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var paper = await _context.PaperInfos
                .Include(p => p.PaperType)
                .Include(p => p.Status)
                .Include(p => p.Files)
                .Include(p => p.AuthoredPapers)
                .ThenInclude(pc => pc.Researcher)
                .Include(p => p.PaperKeywords)
                .ThenInclude(pc => pc.Keyword)
                .FirstOrDefaultAsync(m => m.ID == id);
            if (paper == null)
            {
                return NotFound();
            }

            return View(paper);
        }

        // GET: PaperInfoTest/Create
        public IActionResult Create()
        {
            var paper = new PaperInfo();
            PopulateDropDownLists();
            return View();
        }

        // POST: PaperInfoTest/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("ID,PaperTitle,PaperAbstract,PaperTypeID,PaperLength,StatusID")]PaperInfo paperInfo, IFormFile theFile)
        {
            try
            {
                if (ModelState.IsValid)
                {
                    
                    _context.Add(paperInfo);
                    await _context.SaveChangesAsync();
                    return RedirectToAction(nameof(Index));
                }
            }
            catch (Exception /* dex */)
            {
                ModelState.AddModelError("", "Unable to save changes after multiple attempts. Try again, and if the problem persists, see your system administrator.");
            }

            PopulateDropDownLists();
            return View(paperInfo);
        }

        // GET: PaperInfoTest/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var paper = await _context.PaperInfos
                .Include(p => p.PaperType)
                .Include(p => p.Status)
                .Include(p => p.Files)
                .Include(p => p.AuthoredPapers)
                .ThenInclude(pc => pc.Researcher)
                .Include(p => p.PaperKeywords)
                .ThenInclude(pc => pc.Keyword)
                .SingleOrDefaultAsync(m => m.ID == id);

            if (paper == null)
            {
                return NotFound();
            }
            PopulateDropDownLists();
            return View(paper);
        }

        // POST: PaperInfoTest/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id)
        {
            var paperToUpdate = await _context.PaperInfos
                .Include(p => p.PaperType)
                .Include(p => p.Status)
                .Include(p => p.Files)
                .Include(p => p.AuthoredPapers)
                .ThenInclude(pc => pc.Researcher)
                .Include(p => p.PaperKeywords)
                .ThenInclude(pc => pc.Keyword)
                .SingleOrDefaultAsync(m => m.ID == id);

            if (paperToUpdate == null)
            {
                return NotFound();
            }

            //Try updating it with the values posted
            if (await TryUpdateModelAsync<PaperInfo>(paperToUpdate, "",
                p => p.PaperTitle, p => p.PaperAbstract, p => p.PaperTypeID, p => p.PaperLength, p => p.StatusID))
            {
                try
                {
                    await _context.SaveChangesAsync();
                    return RedirectToAction(nameof(Index));
                }
                catch (RetryLimitExceededException /* dex */)
                {
                    ModelState.AddModelError("", "Unable to save changes after multiple attempts. Try again, and if the problem persists, see your system administrator.");
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!PaperInfoExists(paperToUpdate.ID))
                    {
                        return NotFound();
                    }
                    else
                    {
                        throw;
                    }
                }
            }
            PopulateDropDownLists(paperToUpdate);
            return View(paperToUpdate);
        }

        // GET: PaperInfoTest/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var paperInfo = await _context.PaperInfos
                .Include(p => p.PaperType)
                .Include(p => p.Status)
                .Include(p => p.Files)
                .Include(p => p.AuthoredPapers)
                .ThenInclude(pc => pc.Researcher)
                .Include(p => p.PaperKeywords)
                .ThenInclude(pc => pc.Keyword)
                .FirstOrDefaultAsync(m => m.ID == id);
            if (paperInfo == null)
            {
                return NotFound();
            }

            return View(paperInfo);
        }

        // POST: PaperInfoTest/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var paper = await _context.PaperInfos.FindAsync(id);
            try
            {
                _context.PaperInfos.Remove(paper);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            catch (Exception)
            {
                //Note: there is really no reason a delete should fail if you can "talk" to the database.
                ModelState.AddModelError("", "Unable to save changes. Try again, and if the problem persists see your system administrator.");
            }
            return View(paper);

        }

        private bool PaperInfoExists(int id)
        {
            return _context.PaperInfos.Any(e => e.ID == id);
        }

        private SelectList StatusSelectList(int? id)
        {
            var dQuery = from d in _context.Statuses
                         orderby d.StatName
                         select d;
            return new SelectList(dQuery, "ID", "StatName", id);
        }

        private SelectList PaperTypeSelectList(int? id)
        {
            var dQuery = from d in _context.PaperTypes
                         orderby d.Name
                         select d;
            return new SelectList(dQuery, "ID", "Name", id);
        }

        private void PopulateDropDownLists(PaperInfo infos = null)
        {
            ViewData["StatusID"] = StatusSelectList(infos?.StatusID);
            ViewData["PaperTypeID"] = PaperTypeSelectList(infos?.PaperTypeID);
        }

        
    }
}