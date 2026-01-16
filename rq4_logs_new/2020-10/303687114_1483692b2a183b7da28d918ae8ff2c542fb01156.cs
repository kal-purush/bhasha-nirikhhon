using GPIOInterfaces;
using GPIOInterfaces.Contracts;
using GPIOModels.Configuration;
using GPIOProjects.Base;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace GPIOProjects.Runner
{
    public class ProjectRunner : IProjectRunner
    {
        private readonly List<ProjectConfig> _projects;
        private readonly IServiceProvider _serviceProvider;

        private static List<Task> _runningProjects;

        public ProjectRunner(IOptions<List<ProjectConfig>> options, IServiceProvider serviceProvider)
        {
            _projects = options.Value;
            _serviceProvider = serviceProvider;

            _runningProjects = new List<Task>();
        }

        public Task GetRunningProject()
        {
            throw new NotImplementedException();
        }

        public RunnerResult CreateProjectInstance(string projectName)
        {
            var project = _projects.FirstOrDefault(project => project.Name.ToLower() == projectName.ToLower());

            if (project != null)
            {
                if (project.Runnable)
                {
                    var assemblyTypes = typeof(BaseProject).Assembly.GetTypes();
                    var projectType = assemblyTypes.FirstOrDefault(type => type.Name == project.Name);
                    if (projectType != null)
                    {
                        IProject projectService = (IProject)_serviceProvider.GetService(projectType);
                        var runningProject = Task.Run(() => projectService.RunProject());
                        _runningProjects.Add(runningProject);

                        return RunnerResult.Success;
                    }
                    else
                        return RunnerResult.ClassNotFound;
                }
                else
                    return RunnerResult.ProjectNotRunnable;
            }
            else
                return RunnerResult.UnknownProject;
        }
    }
}