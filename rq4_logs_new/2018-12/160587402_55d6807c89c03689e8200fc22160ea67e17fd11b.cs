using System;
using Oesia.Models;
using System.Net.Http;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Oesia.Data;

namespace Oesia.Services
{
    public class OesiaServices
    {
        public OesiaServices()
        {
        }
        private readonly ApplicationDbContext _context;

        public OesiaServices(ApplicationDbContext context)
        {
            _context = context;
        }

        public List<Project> GetProjectsDB()
        {
            List<Project> projects = _context.Project.ToList();
            return projects;
        }

        public List<Module> GetModulesDB()
        {
            List<Module> modules = _context.Module.ToList();
            return modules;
        }

        public List<Submodule> GetSubmodulesDB()
        {
            List<Submodule> submodules = _context.Submodule.ToList();
            return submodules;
        }

        public List<Oesia.Models.Task> GetTasksDB()
        {
            List<Oesia.Models.Task> tasks = _context.Task.ToList();
            return tasks;
        }

        public List<Subtask> GetSubtasksDB()
        {
            List<Subtask> subtasks = _context.Subtask.ToList();
            return subtasks;
        }
    }
}