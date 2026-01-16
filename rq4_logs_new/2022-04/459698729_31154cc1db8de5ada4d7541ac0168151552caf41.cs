using PandoraMVC;
using PandoraMVC.Controllers;
using PandoraMVC.Helpers;
using System;
using System.Collections.Generic;
using Xunit;

namespace UnitTesting
{
    public class ActionRecognitionTests
    {
        private GanttDataSource epic;
        private GanttDataSource task;


        public ActionRecognitionTests()
        {
            task = new GanttDataSource()
            {
                taskId = 1,
                taskName = "sds",
                startDate = new DateTime(2022, 2, 1),
                endDate = new DateTime(2022, 2, 5),
                duration = 4,
                progress = 0,
                subTasks = new List<GanttDataSource>() { },
                isEpic = false
            };

            epic = new GanttDataSource()
            {
                taskId = 1,
                taskName = "sds",
                startDate = new DateTime(2022, 2, 1),
                endDate = new DateTime(2022, 2, 5),
                duration = 4,
                progress = 0,
                subTasks = new List<GanttDataSource>() { task },
                isEpic = true
            };
        }

        [Fact]
        public void ActionRecognizer_ActionIsAddTask_ReturnActionAddTask()
        {
            //Arrange
            var recognizer = new ActionRecognizer();

            var data = new ICRUDModel<GanttDataSource>()
            {
                added = new List<GanttDataSource>() { task },
                changed = new List<GanttDataSource>() { epic },
                deleted = new List<GanttDataSource>()
            };

            //Act
            UiAction action = recognizer.RecognizeAction(data);

            //Assert
            Assert.Equal(UiAction.AddTask, action);
        }

        [Fact]
        public void ActionRecognizer_ActionIsAddEpic_ReturnActionAddEpic()
        {
            //Arrange
            var recognizer = new ActionRecognizer();

            var data = new ICRUDModel<GanttDataSource>()
            {
                added = new List<GanttDataSource>() { epic },
                changed = new List<GanttDataSource>(),
                deleted = new List<GanttDataSource>()
            };

            //Act
            UiAction action = recognizer.RecognizeAction(data);

            //Assert
            Assert.Equal(UiAction.AddEpic, action);
        }

        [Fact]
        public void ActionRecognizer_ActionIsEditTask_ReturnActionEditTask()
        {
            //Arrange
            var recognizer = new ActionRecognizer();

            var data = new ICRUDModel<GanttDataSource>()
            {
                added = new List<GanttDataSource>(),
                changed = new List<GanttDataSource>() { epic, task },
                deleted = new List<GanttDataSource>()
            };

            //Act
            UiAction action = recognizer.RecognizeAction(data);

            //Assert
            Assert.Equal(UiAction.EditTask, action);
        }

        [Fact]
        public void ActionRecognizer_ActionIsEditEpic_ReturnActionEditEpic()
        {
            //Arrange
            var recognizer = new ActionRecognizer();

            var data = new ICRUDModel<GanttDataSource>()
            {
                added = new List<GanttDataSource>(),
                changed = new List<GanttDataSource>() { epic },
                deleted = new List<GanttDataSource>()
            };

            //Act
            UiAction action = recognizer.RecognizeAction(data);

            //Assert
            Assert.Equal(UiAction.EditEpic, action);
        }

        [Fact]
        public void ActionRecognizer_ActionIsDeleteTask_ReturnActionDeleteTask()
        {
            //Arrange
            var recognizer = new ActionRecognizer();

            var data = new ICRUDModel<GanttDataSource>()
            {
                added = new List<GanttDataSource>(),
                changed = new List<GanttDataSource>() { epic },
                deleted = new List<GanttDataSource>() { task }
            };

            //Act
            UiAction action = recognizer.RecognizeAction(data);

            //Assert
            Assert.Equal(UiAction.DeleteTask, action);
        }

        [Fact]
        public void ActionRecognizer_ActionIsDeleteEpic_ReturnActionDeleteEpic()
        {
            //Arrange
            var recognizer = new ActionRecognizer();

            var data = new ICRUDModel<GanttDataSource>()
            {
                added = new List<GanttDataSource>(),
                changed = new List<GanttDataSource>(),
                deleted = new List<GanttDataSource>() { epic }
            };

            //Act
            UiAction action = recognizer.RecognizeAction(data);

            //Assert
            Assert.Equal(UiAction.DeleteEpic, action);
        }

        [Fact]
        public void ActionRecognizer_ActionIsNewlyAddedEditTask_ReturnActionEditTask()
        {
            //Arrange
            var recognizer = new ActionRecognizer();

            var newTaskWhichIsLyingThatHeIsAnEpic = new GanttDataSource()
            {
                taskId = 1,
                taskName = "sds",
                startDate = new DateTime(2022, 2, 1),
                endDate = new DateTime(2022, 2, 5),
                duration = 4,
                progress = 0,
                subTasks = new List<GanttDataSource>() { },
                isEpic = true //LIER!
            };

            var data = new ICRUDModel<GanttDataSource>()
            {
                added = new List<GanttDataSource>(),
                changed = new List<GanttDataSource>() { epic, newTaskWhichIsLyingThatHeIsAnEpic },
                deleted = new List<GanttDataSource>()
            };

            //Act
            UiAction action = recognizer.RecognizeAction(data);

            //Assert
            Assert.Equal(UiAction.EditTask, action);
        }
    }
}
