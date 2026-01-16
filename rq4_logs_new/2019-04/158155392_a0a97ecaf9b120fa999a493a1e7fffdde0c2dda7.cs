using DataAccessLayer;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServiceLayer.Services
{
    public class HelpWantedService
    {
        private readonly BroadwayBuilderContext _dbContext;

        public HelpWantedService(BroadwayBuilderContext dbContext)
        {
            this._dbContext = dbContext;
        }

        public void CreateTheaterJob(TheaterJobPosting theaterJob)
        {
            theaterJob.DateCreated = DateTime.Now;
            _dbContext.TheaterJobPostings.Add(theaterJob);
        }

        public TheaterJobPosting GetTheaterJob(TheaterJobPosting theaterJob)
        {
            return _dbContext.TheaterJobPostings.Find(theaterJob.HelpWantedID);
        }

        public TheaterJobPosting GetTheaterJob(int helpwantedid)
        {
            return _dbContext.TheaterJobPostings.Find(helpwantedid);
        }

        public int GetTheaterJobsCount(int theaterid)
        {
            return _dbContext.TheaterJobPostings.Where(job => job.TheaterID == theaterid).Count();
            
        }

        public IEnumerable GetAllJobsFromTheater(int theaterid, int currentPage, int numberOfItems)
        {
            // Starting point of query to get data from the theater job posting table
            var startingPoint = numberOfItems * (currentPage - 1);

            return _dbContext.TheaterJobPostings
                // Order by newest data first
                .OrderByDescending(job => job.DateCreated)
                // Gets jobs for just a specific theater
                .Where(job => job.TheaterID == theaterid)
                // Skip this many ahead
                .Skip(startingPoint)
                // Take all the items that was skipped
                .Take(numberOfItems)
                // Select specific data for the response
                .Select(job => new
            {
                Title = job.Title,
                Position = job.Position,
                JobType = job.JobType,
                Hours = job.Hours,
                Description = job.Description,
                Requirements = job.Requirements,
                DateCreated = job.DateCreated,
                HelpWantedId = job.HelpWantedID,
                TheaterId = job.TheaterID
            }).ToList();
        }

        public IEnumerable FilterTheaterJobPostingFromTheater(int theaterid,string title,string Postion,string Hours, string description, string requirements,DateTime date)
        {
            //IQueryable allJobsFromTheater = GetAllJobsForTheater(theaterid);
            var list = _dbContext.TheaterJobPostings.Where(job => job.TheaterID == theaterid)
                    .Select(job => new
                    {
                        Title = job.Title,
                        Position = job.Position,
                        Hours = job.Hours,
                        Description = job.Description,
                        Requirements = job.Requirements,
                        DateCreated = job.DateCreated,
                        HelpWantedId = job.HelpWantedID,
                        TheaterId = job.TheaterID
                    });
            if (String.IsNullOrEmpty(title))
            {
                list = list.Where(job=>job.Title == title);
            }
            if (String.IsNullOrEmpty(Postion))
            {
                list = list.Where(job => job.Position == Postion);
            }
            return list;
            
        }

        public void UpdateTheaterJob(TheaterJobPosting updatedTheaterJob)
        {
            _dbContext.Entry(updatedTheaterJob).State = EntityState.Modified;
        }

        public void DeleteTheaterJob(TheaterJobPosting theaterJob)
        {
            TheaterJobPosting jobToRemove = _dbContext.TheaterJobPostings.Find(theaterJob.HelpWantedID);
            if (jobToRemove != null)
            {
                _dbContext.TheaterJobPostings.Remove(jobToRemove);
            }
        }
    }
}