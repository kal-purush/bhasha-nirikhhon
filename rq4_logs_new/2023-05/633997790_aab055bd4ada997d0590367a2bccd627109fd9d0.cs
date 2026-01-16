using cursuri_studenti.course.model;
using cursuri_studenti.course.service;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace tests_cursuri_studenti.Tests
{
    public class TestCourse
    {
        // Testing Course Service

        [Fact]
        public void FindById_ValidMatch_ReturnsCourse()
        {
            // Arrange
            int Id = 123;
            Course expectedCourse = new Course(Id, 123, "coursename");
            List<Course> list = new List<Course> { expectedCourse };
            CourseService courseService = new CourseService(list);

            // Act
            Course actualCourse = courseService.FindById(Id);

            // Assert
            Assert.NotNull(actualCourse);
            Assert.Equal(expectedCourse.Id, actualCourse.Id);
            Assert.Equal(expectedCourse.Name, actualCourse.Name);
            Assert.Equal(expectedCourse.ProfessorId, actualCourse.ProfessorId);
        }

        [Fact]
        public void FindById_NoMatch_ReturnsNull()
        {
            // Arrange
            int Id = 123;
            List<Course> list = new List<Course>();
            CourseService courseService = new CourseService(list);

            // Act
            Course actualCourse = courseService.FindById(Id);

            // Assert
            Assert.Null(actualCourse);
        }

        [Fact]
        public void FindById_MultipleCourses_ReturnsCorrectCourse()
        {
            // Arrange
            int Id = 123;
            Course expectedCourse = new Course(Id, 123, "coursename");
            Course anotherCourse = new Course(101, 100, "justaname");
            List<Course> list = new List<Course> { expectedCourse, anotherCourse };
            CourseService courseService = new CourseService(list);

            // Act
            Course actualCourse = courseService.FindById(Id);

            // Assert
            Assert.NotNull(actualCourse);
            Assert.Equal(expectedCourse.Id, actualCourse.Id);
            Assert.Equal(expectedCourse.Name, actualCourse.Name);
            Assert.Equal(expectedCourse.ProfessorId, actualCourse.ProfessorId);
        }

        [Fact]
        public void FindByProfessorId_ValidMatch_ReturnsCourse()
        {
            // Arrange
            int ProfessorId = 123;
            Course expectedCourse = new Course(123, ProfessorId, "name");
            List<Course> list = new List<Course> { expectedCourse };
            CourseService courseService = new CourseService(list);

            // Act
            List<Course> courses = courseService.FindByProfessorId(ProfessorId);

            // Assert
            Assert.NotNull(courses[0]);
            Assert.Equal(expectedCourse.Id, courses[0].Id);
            Assert.Equal(expectedCourse.Name, courses[0].Name);
            Assert.Equal(expectedCourse.ProfessorId, courses[0].ProfessorId);
        }

        [Fact]
        public void FindByProfessorId_NoMatch_ReturnsNull()
        {
            // Arrange
            int ProfessorId = 123;
            List<Course> list = new List<Course>();
            CourseService courseService = new CourseService(list);

            // Act
            List<Course> courses = courseService.FindByProfessorId(ProfessorId);

            // Assert
            Assert.Empty(courses);
        }

        [Fact]
        public void FindByProfessorId_MultipleCourses_ReturnsCorrectCourse()
        {
            // Arrange
            int ProfessorId = 123;
            Course expectedCourse = new Course(123, ProfessorId, "name");
            Course anotherCourse = new Course(101, 100, "justaname");
            List<Course> list = new List<Course> { expectedCourse, anotherCourse };
            CourseService courseService = new CourseService(list);

            // Act
            List<Course> courses = courseService.FindByProfessorId(ProfessorId);

            // Assert
            Assert.NotNull(courses[0]);
            Assert.Equal(expectedCourse.Id, courses[0].Id);
            Assert.Equal(expectedCourse.Name, courses[0].Name);
            Assert.Equal(expectedCourse.ProfessorId, courses[0].ProfessorId);
        }

        [Fact]
        public void FindByName_ValidMatch_ReturnsCourse()
        {
            // Arrange
            string Name = "name";
            Course expectedCourse = new Course(123, 123, Name);
            List<Course> list = new List<Course> { expectedCourse };
            CourseService courseService = new CourseService(list);

            // Act
            Course actualCourse = courseService.FindByName(Name);

            // Assert
            Assert.NotNull(actualCourse);
            Assert.Equal(expectedCourse.Id, actualCourse.Id);
            Assert.Equal(expectedCourse.Name, actualCourse.Name);
            Assert.Equal(expectedCourse.ProfessorId, actualCourse.ProfessorId);
        }

        [Fact]
        public void FindByName_NoMatch_ReturnsNull()
        {
            // Arrange
            string Name = "name";
            List<Course> list = new List<Course>();
            CourseService courseService = new CourseService(list);

            // Act
            Course actualCourse = courseService.FindByName(Name);

            // Assert
            Assert.Null(actualCourse);
        }

        [Fact]
        public void FindByName_MultipleCourses_ReturnsCorrectCourse()
        {
            // Arrange
            string Name = "name";
            Course expectedCourse = new Course(123, 123, Name);
            Course anotherCourse = new Course(101, 100, "justaname");
            List<Course> list = new List<Course> { expectedCourse, anotherCourse };
            CourseService courseService = new CourseService(list);

            // Act
            Course actualCourse = courseService.FindByName(Name);

            // Assert
            Assert.NotNull(actualCourse);
            Assert.Equal(expectedCourse.Id, actualCourse.Id);
            Assert.Equal(expectedCourse.Name, actualCourse.Name);
            Assert.Equal(expectedCourse.ProfessorId, actualCourse.ProfessorId);
        }

        [Fact]
        public void NewId_ReturnsUnusedId()
        {
            // Arrange
            int Id1 = 12, Id2 = 101;
            Course aCourse = new Course(Id1, 111, "da2");
            Course anotherCourse = new Course(Id2, 222, "fasf");
            List<Course> list = new List<Course> { aCourse, anotherCourse };
            CourseService courseService = new CourseService(list);

            // Act
            int newId = courseService.NewId();

            // Assert
            Assert.Null(courseService.FindById(newId));
            Assert.NotEqual(Id1, newId);
            Assert.NotEqual(Id2, newId);
            Assert.InRange(newId, 1, 9999);
        }

        // TODO: REMOVE, ADD, EDIT

        [Fact]
        public void GetCount_ReturnsActualCount()
        {
            // Arrange
            Course aCourse = new Course(123, 123, "courseName");
            List<Course> list = new List<Course> { aCourse };
            CourseService courseService = new CourseService(list);

            // Act
            int count = courseService.GetCount();

            // Assert
            Assert.Equal(list.Count(), count);
        }

        [Fact]
        public void ClearList_ListRemainsEmpty()
        {
            // Arrange
            Course aCourse = new Course(123, 123, "courseName");
            List<Course> list = new List<Course> { aCourse };
            CourseService courseService = new CourseService(list);

            // Act
            courseService.ClearList();

            // Assert
            Assert.Equal(0, courseService.GetCount());
            Assert.Empty(courseService.GetList());
        }

        [Fact]
        public void GetList_ReturnsActualList()
        {
            // Arrange
            Course aCourse = new Course(123, 123, "courseName");
            List<Course> expectedList = new List<Course> { aCourse };
            CourseService courseService = new CourseService(expectedList);

            // Act
            List<Course> actualList = courseService.GetList();

            // Assert
            Assert.Equal(expectedList, actualList);
            Assert.Equal(expectedList.Count(), actualList.Count());
        }

        [Fact]
        public void SetList_EditsList()
        {
            // Arrange
            Course aCourse = new Course(123, 123, "courseName");
            List<Course> list = new List<Course>();
            List<Course> setList = new List<Course> { aCourse };
            CourseService courseService = new CourseService(list);

            // Act
            courseService.SetList(setList);

            // Assert
            Assert.Equal(setList, courseService.GetList());
            Assert.Equal(setList.Count(), courseService.GetCount());
            Assert.NotEqual(list, courseService.GetList());
        }
    }
}