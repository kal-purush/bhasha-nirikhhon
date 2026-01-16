using AutoMapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ProjectManagementSystem.API.Repositories.UnitOfWork;
using ProjectManagementSystem.API.Services;
using ProjectManagementSystem.BusinessLogicLayer.Services;
using ProjectManagementSystem.DataAccessLayer.Repository.UnitOfWork;
using ProjectManagementSystem.Draft;
using ProjectManagementSystem.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProjectManagementSystem.BusinessLogicLayer.Tests
{
    [TestClass]
    public class ProjectServiceTest
    {
        private const string name = "Test Project";

        private static int count;

        private IProjectService projectService;
        private IUnitOfWork uow;

        [TestInitialize]
        public void Init()
        {
            uow = new UnitOfWork("UnitTestDB");
            projectService = new ProjectService(uow);
            Mapper.Initialize(cfg => cfg.CreateMap<CreateProjectDraft, Project>());
        }

        [TestMethod]
        public void CreateProjectTest()
        {
            int firstCount = uow.Projects.GetAll().Count();
            CreateProjectDraft draft = new CreateProjectDraft()
            {
                Name = "Test" + DateTime.Now,
                StartDate = DateTime.Now,
                FinishDate = DateTime.Now.AddDays(60.0)
            };
            projectService.CreateProject(draft);
            Assert.AreEqual(firstCount + 1, uow.Projects.GetAll().Count());
        }

        [TestMethod]
        public void DeleteProjectTest()
        {
            Project project = SaveProject();
            projectService.DeleteProject(project.Id);
            Assert.IsNull(projectService.GetProjectById(project.Id));
        }

        [TestMethod]
        public void FinishProject()
        {
            Project project = SaveProject();
            projectService.FinishProject(project.Id);
            Assert.AreEqual(projectService.GetProjectById(project.Id).Status, ProjectStatus.Finished);
        }

        [TestMethod]
        public void GetCurrentProjectUsersTest()
        {

        }
        

        private Project SaveProject()
        {
            Project p = new Project()
            {
                Name = name + (++count).ToString(),
                StartDate = DateTime.MinValue,
                FinishDate = DateTime.MaxValue,
                Status = ProjectStatus.During
            };

            uow.Projects.Add(p);
            uow.Save();
            return p;
        }
    }
}