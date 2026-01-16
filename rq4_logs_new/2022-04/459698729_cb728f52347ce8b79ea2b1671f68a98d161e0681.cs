using PandoraMVC.Data;
using PandoraMVC.Entities;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;

namespace PandoraMVC.Services
{
    public class EpicService
    {
        private readonly ApplicationDbContext _context;

        public EpicService(ApplicationDbContext applicationDbContext)
        {
            _context = applicationDbContext;
        }

        public IEnumerable<GanttDataSource> GetGanttDataSources()
        {
            IEnumerable<Epic> epics = GetEpicsWithTasks(); //TODO: add filtering by workspace

            IEnumerable<GanttDataSource> ganttSources = ConvertEpicsToGanttDataSources(epics);

            return ganttSources;
        }

        private IEnumerable<Epic> GetEpicsWithTasks() => _context.Epics.Include(e => e.Tasks).ToList();

        private IEnumerable<GanttDataSource> ConvertEpicsToGanttDataSources(IEnumerable<Epic> epics) 
            => epics.Select(e => ConvertEpicToGanttDataSource(e));

        private GanttDataSource ConvertEpicToGanttDataSource(Epic epic)
        {
            var tasks = _context.Tasks.Where(t => t.EpicId == epic.Id);

            var epicSource = new GanttDataSource()
            {
                TaskId = epic.Id,
                TaskName = epic.Name
            };

            var convertedTasks = tasks.Select(t => new GanttDataSource() 
            {
                TaskId = t.Id,
                TaskName = t.Description,
                StartDate = t.StartDate,
                EndDate = t.EndDate,
                Progress = t.Status == true? 100 : 0
            });

            epicSource.SubTasks = convertedTasks.ToList();

            return epicSource;
        }
    }
}