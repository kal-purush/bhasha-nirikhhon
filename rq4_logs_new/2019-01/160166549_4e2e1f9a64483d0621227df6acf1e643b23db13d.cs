using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Net;

using Common.ModelsDTO;
using gtdtimer.Controllers;
using Moq;
using NUnit.Framework;
using ServiceTier.Services;
using Timer.DAL.Timer.DAL.UnitOfWork;

namespace gtdtimerTests.Controllers
{
    [TestFixture]
    public class TasksControllerTests : ControllerBase
    {
        private Mock<ITaskService> taskService;
        private Mock<IUserIdentityService> userIdentityService;
        private Mock<IUnitOfWork> unitOfWork;

        private TasksController subject;

        [SetUp]
        public void Setup()
        {
            taskService = new Mock<ITaskService>();
            userIdentityService = new Mock<IUserIdentityService>();
            unitOfWork = new Mock<IUnitOfWork>();
            subject = new TasksController(taskService.Object, userIdentityService.Object);
        }

        [Test]
        public void GetTaskById()
        {
            int taskId = 9;
            TaskDTO taskDTO = new TaskDTO();
            taskService.Setup(task => task.GetTaskById(taskId)).Returns(taskDTO);

            var actual = (OkObjectResult)subject.GetTaskById(taskId);

            Assert.AreEqual(actual.StatusCode, (int)HttpStatusCode.OK);
            Assert.AreSame(actual.Value, taskDTO);
        }

        [Test]
        public void GetAllTasks()
        {
            List<TaskDTO> tasks = new List<TaskDTO>();
            taskService.Setup(_ => _.GetAllTasks()).Returns(tasks);
            var actual = (OkObjectResult)subject.GetAllTasks();

            Assert.AreEqual(actual.StatusCode, (int)HttpStatusCode.OK);
            Assert.AreSame(actual.Value, tasks);
        }

        [Test]
        public void GetAllActiveTasks()
        {
            List<TaskDTO> tasks = new List<TaskDTO>();
            taskService.Setup(_ => _.GetAllActiveTasks()).Returns(tasks);
            var actual = (OkObjectResult)subject.GetAllActiveTasks();

            Assert.AreEqual(actual.StatusCode, (int)HttpStatusCode.OK);
            Assert.AreSame(actual.Value, tasks);
        }

        [Test]
        public void GetAllArchivedTasks()
        {
            List<TaskDTO> tasks = new List<TaskDTO>();
            taskService.Setup(_ => _.GetAllArchivedTasks()).Returns(tasks);
            var actual = (OkObjectResult)subject.GetAllArchivedTasks();

            Assert.AreEqual(actual.StatusCode, (int)HttpStatusCode.OK);
            Assert.AreSame(actual.Value, tasks);
        }

        [Test]
        public void GetAllTasksByUserId()
        {
            List<TaskDTO> tasks = new List<TaskDTO>();
            var userId = 315;
            userIdentityService.Setup(_ => _.GetUserId()).Returns(userId);
            taskService.Setup(_ => _.GetAllTasksByUserId(userId)).Returns(tasks);
            var actual = (OkObjectResult)subject.GetAllTasksByUserId();

            Assert.AreEqual(actual.StatusCode, (int)HttpStatusCode.OK);
            Assert.AreSame(actual.Value, tasks);
        }

        [Test]
        public void GetAllActiveTasksByUserId()
        {
            List<TaskDTO> tasks = new List<TaskDTO>();
            var userId = 315;
            userIdentityService.Setup(_ => _.GetUserId()).Returns(userId);
            taskService.Setup(_ => _.GetAllActiveTasksByUserId(userId)).Returns(tasks);
            var actual = (OkObjectResult)subject.GetAllActiveTasksByUserId();

            Assert.AreEqual(actual.StatusCode, (int)HttpStatusCode.OK);
            Assert.AreSame(actual.Value, tasks);
        }

        [Test]
        public void GetAllArchivedTasksByUserId()
        {
            List<TaskDTO> tasks = new List<TaskDTO>();
            var userId = 315;
            userIdentityService.Setup(_ => _.GetUserId()).Returns(userId);
            taskService.Setup(_ => _.GetAllArchivedTasksByUserId(userId)).Returns(tasks);
            var actual = (OkObjectResult)subject.GetAllArchivedTasksByUserId();

            Assert.AreEqual(actual.StatusCode, (int)HttpStatusCode.OK);
            Assert.AreSame(actual.Value, tasks);
        }

        [Test]
        public void CreateTask()
        {
            var userId = 315;
            userIdentityService.Setup(_ => _.GetUserId()).Returns(userId);
            TaskDTO model = new TaskDTO();
            var actual = (OkResult)subject.CreateTask(model);

            Assert.AreEqual(actual.StatusCode, (int)HttpStatusCode.OK);
            taskService.Verify(_ => _.CreateTask(model), Times.Once);
        }

        [Test]
        public void UpdateTask()
        {
            var userId = 315;
            userIdentityService.Setup(_ => _.GetUserId()).Returns(userId);
            TaskDTO model = new TaskDTO();
            var actual = (OkResult)subject.UpdateTask(model);

            Assert.AreEqual(actual.StatusCode, (int)HttpStatusCode.OK);
            taskService.Verify(_ => _.UpdateTask(model), Times.Once);
        }

        [Test]
        public void DeleteTask()
        {
            var taskId = 2;
            var actual = (OkResult)subject.DeleteTask(taskId);

            Assert.AreEqual(actual.StatusCode, (int)HttpStatusCode.OK);
            taskService.Verify(_ => _.DeleteTaskById(taskId), Times.Once);
        }

        [Test]
        public void UpdateTaskStatus()
        {
            var userId = 315;
            userIdentityService.Setup(_ => _.GetUserId()).Returns(userId);
            TaskDTO model = new TaskDTO();
            var actual = (OkResult)subject.UpdateTaskStatus(model);

            Assert.AreEqual(actual.StatusCode, (int)HttpStatusCode.OK);
            taskService.Verify(_ => _.UpdateTaskStatus(model), Times.Once);
        }

        [Test]
        public void ResetTask()
        {
            var taskId = 2;
            var actual = (OkResult)subject.ResetTask(taskId);

            Assert.AreEqual(actual.StatusCode, (int)HttpStatusCode.OK);
            taskService.Verify(_ => _.ResetTask(taskId), Times.Once);
        }

        [Test]
        public void StartTask()
        {
            var userId = 315;
            userIdentityService.Setup(_ => _.GetUserId()).Returns(userId);
            TaskDTO model = new TaskDTO();
            var actual = (OkObjectResult)subject.StartTask(model);

            Assert.AreEqual(actual.StatusCode, (int)HttpStatusCode.OK);
            taskService.Verify(_ => _.StartTask(model), Times.Once);
            Assert.That(actual.Value, Is.EqualTo("Timer has started."));
        }

        [Test]
        public void PauseTask()
        {
            var userId = 315;
            userIdentityService.Setup(_ => _.GetUserId()).Returns(userId);
            TaskDTO model = new TaskDTO();
            var actual = (OkObjectResult)subject.PauseTask(model);

            Assert.AreEqual(actual.StatusCode, (int)HttpStatusCode.OK);
            taskService.Verify(_ => _.PauseTask(model), Times.Once);
            Assert.That(actual.Value, Is.EqualTo("Timer has been paused."));
        }
    }
}