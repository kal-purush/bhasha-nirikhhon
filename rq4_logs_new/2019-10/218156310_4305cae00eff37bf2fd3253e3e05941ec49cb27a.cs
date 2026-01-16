using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using HeadCount.Data;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.EntityFrameworkCore;

namespace HeadCount.Pages.Employees
{
    public class IndexModel : PageModel
    {
        private readonly HeadCount.Data.ApplicationDbContext _context;
        public object searchDate;
        public DateTime? deadline;

        [BindProperty]
        public int OpenCount { get; set; }

        [BindProperty]
        public int ClosedCount { get; set; }

        [BindProperty(SupportsGet = true)]
        public string SearchInput { get; set; } = "";

        public System.DateTime SearchDate { get; set; }

        public IndexModel(HeadCount.Data.ApplicationDbContext context)
        {
            _context = context;
        }

        //public PaginatedList<Employee> Employee { get; set; }
        public DateTime? ReleaseDate { get; private set; }
        public string CurrentSort { get; private set; }
        public string NameSort { get; private set; }
        public string DateSort { get; private set; }
        public string CurrentFilter { get; private set; }
        public PaginatedList<Employee> Employee { get; set; }
        public async Task OnGet(string sortOrder, string currentFilter,string searchString,int? pageIndex)
        {
            CurrentSort = sortOrder;
            NameSort = String.IsNullOrEmpty(sortOrder) ? "name_desc" : "";
            DateSort = sortOrder == "Date" ? "date_desc" : "Date";
            if (searchString != null)
            {
                pageIndex = 1;
            }
            else
            {
                searchString = currentFilter;
            }

            CurrentFilter = searchString;

            IQueryable<Employee> emps = emps = from m in _context.Employee select m;




           if(!String.IsNullOrEmpty(searchString))

            
            {
                emps = emps.Where(s => s.FName.Contains(searchString)
                    ||s.LName.Contains(searchString));
            }
            switch (sortOrder)
            {


                case "name_desc":
                    emps = emps.OrderByDescending(s => s.FName);
                    break;
                case "Date":
                    emps = emps.OrderBy(s => s.LName);
                    break;
                case "date_desc":
                    emps = emps.OrderByDescending(s => s.FName);
                    break;
                default:
                     emps = emps.OrderBy(s => s.FName);
                    break;
            }
            int pageSize = 3;
            Employee = await PaginatedList<Employee>.CreateAsync(emps.AsNoTracking(), pageIndex ?? 1, pageSize);

           
            var startDate = new System.DateTime(2019, 10, 17);

            var endDate = new System.DateTime(2019, 10, 22);
           // Employee = await EmpsToListAsync();

            //after getting movies then due count
            foreach (var item in emps)
            {
                if (item.ApprPos == false)
                {
                    OpenCount += 1;
                }
                else if (item.ApprPos == true)
                {
                    ClosedCount += 1;
                }
                else
                {
                    // don't count if null
                }
            }
            System.Diagnostics.Debug.WriteLine("yoyoCount: ");
            System.Diagnostics.Debug.WriteLine(OpenCount);
            System.Diagnostics.Debug.WriteLine("closedCount: ");
            System.Diagnostics.Debug.WriteLine(ClosedCount);
        }

        private Task<PaginatedList<Employee>> EmpsToListAsync()
        {
            throw new NotImplementedException();
        }

        //private Task<PaginatedList<Employee>> EmpsToListAsync()

        
        }
    }

   
    