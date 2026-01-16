using Common.Exceptions;
using Common.ModelsDTO;
using Moq;
using NUnit.Framework;
using ServiceTier.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using Timer.DAL.Timer.DAL.Entities;
using Timer.DAL.Timer.DAL.Repositories;
using Timer.DAL.Timer.DAL.UnitOfWork;

namespace ServiceTierTests
{
    public class TaskServiceTests
    {
        Tasks task = new Tasks {Id = 1, Name="Test", UserId = 1};
        List<Tasks> tasks = new List<Tasks>();
        int userId = 1;
        private Mock<IUnitOfWork> unitOfWork;
        private TaskService subject;

        [SetUp]
        public void Setup()
        {
            unitOfWork = new Mock<IUnitOfWork>();
            subject = new TaskService(unitOfWork.Object);
            tasks.Add(task);
        }

        [Test]
        public void CreateTask()
        {
            TaskDTO task = new TaskDTO();
            var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);
            subject.CreateTask(task);

            unitOfWork.Verify(_ => _.Save(), Times.Once);
        }

        [Test]
        public void UpdateTask()
        {
            TaskDTO task = new TaskDTO();
            var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);
            subject.UpdateTask(task);

            unitOfWork.Verify(_ => _.Save(), Times.Once);
        }

        [Test]
        public void DeleteTask()
        {
            int taskId = 2;
            Tasks task = new Tasks();
            var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);
            unitOfWork.Setup(_ => _.Tasks.GetByID(taskId)).Returns(task);

            subject.DeleteTaskById(taskId);

            unitOfWork.Verify(_ => _.Save(), Times.Once);
        }

        [Test]
        public void GetTaskById()
        {
            int taskId = 2;
            Tasks task = new Tasks();
            var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);
            unitOfWork.Setup(_ => _.Tasks.GetByID(taskId)).Returns(task);

            Assert.AreEqual(subject.GetTaskById(taskId).Name, task.Name);
        }

        [Test]
        public void UpdateTaskStatus()
        {
            TaskDTO task = new TaskDTO();
            var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);
            subject.SwitchArchivedStatus(task);

            unitOfWork.Verify(_ => _.Save(), Times.Once);
        }

        [Test]
        public void ResetTask()
        {
            int taskId = 2;
            Tasks task = new Tasks();
            var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);
            unitOfWork.Setup(_ => _.Tasks.GetByID(taskId)).Returns(task);

            subject.ResetTask(taskId);

            unitOfWork.Verify(_ => _.Save(), Times.Once);
        }

        [Test]
        public void StartTask()
        {
            TaskDTO task = new TaskDTO();
            var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);
            subject.StartTask(task);

            unitOfWork.Verify(_ => _.Save(), Times.Once);
        }

        [Test]
        public void PauseTask()
        {
            TaskDTO task = new TaskDTO();
            var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);
            subject.PauseTask(task);

            unitOfWork.Verify(_ => _.Save(), Times.Once);
        }

        [Test]
        public void Get_ThrowsTaskNotFoundExceoption()
        {
            int taskId = 500;
            var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);

            var ex = Assert.Throws<TaskNotFoundException>(() => subject.GetTaskById(taskId));

            Assert.That(ex.Message, Is.EqualTo("Task does not exist!"));
        }
        
        [Test]
        public void GetAllTasks()
        {
             var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);
            unitOfWork.Setup(_ => _.Tasks.GetAllEntities()).Returns(tasks);

            Assert.AreEqual(subject.GetAllTasks().ToList()[0].Name, task.Name);
        }

        [Test]
        public void GetAllTasksByUserId()
        {
            var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);
            unitOfWork.Setup(_ => _.Tasks.GetAllEntitiesByFilter(It.IsAny<Func<Tasks, bool>>())).Returns(tasks);

            Assert.AreEqual(subject.GetAllTasksByUserId(userId).ToList()[0].Name, task.Name);
        }

        [Test]
        public void GetAllActiveTasks()
        {
            var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);
            unitOfWork.Setup(_ => _.Tasks.GetAllEntitiesByFilter(It.IsAny<Func<Tasks, bool>>())).Returns(tasks);

            Assert.AreEqual(subject.GetAllActiveTasks().ToList()[0].Name, task.Name);
        }

        [Test]
        public void GetAllActiveTasksByUserId()
        {
            var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);
            unitOfWork.Setup(_ => _.Tasks.GetAllEntitiesByFilter(It.IsAny<Func<Tasks, bool>>())).Returns(tasks);

            Assert.AreEqual(subject.GetAllActiveTasksByUserId(userId).ToList()[0].Name, task.Name);
        }

        [Test]
        public void GetAllArchivedTasks()
        {
            var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);
            unitOfWork.Setup(_ => _.Tasks.GetAllEntitiesByFilter(It.IsAny<Func<Tasks, bool>>())).Returns(tasks);

            Assert.AreEqual(subject.GetAllArchivedTasks().ToList()[0].Name, task.Name);
        }

        [Test]
        public void GetAllArchivedTasksByUserId()
        {
            var taskRepository = new Mock<IRepository<Tasks>>();

            unitOfWork.Setup(_ => _.Tasks).Returns(taskRepository.Object);
            unitOfWork.Setup(_ => _.Tasks.GetAllEntitiesByFilter(It.IsAny<Func<Tasks, bool>>())).Returns(tasks);

            Assert.AreEqual(subject.GetAllArchivedTasksByUserId(userId).ToList()[0].Name, task.Name);
        }
    }
}