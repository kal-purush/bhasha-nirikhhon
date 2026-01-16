using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using OnlineMarks.Data.Models;
using OnlineMarks.Data.ViewModels.Subjects;
using OnlineMarks.Interfaces.Maps;
using OnlineMarks.Interfaces.Repository;
using OnlineMarks.Interfaces.Services;
using System;

namespace OnlineMarks.Services.UT
{
    [TestClass]
    public class SubjectServiceUT
    {

        private SubjectService _service;

        private SubjectView _subject;
        private string _subjectName;

        public string _profesorName;

        private  Mock<ISubjectRepository> _subjectRepositoryMock;
        private  Mock<ISubjectViewSubjectMap> _subjectViewUserMap;
        private  Mock<IProfessorRepository> _professorRepository;
        private  Mock<IStudentRepository> _studentRepository;


        [TestInitialize]
        public void Init()
        {
            SetupData();
            SetupMock();
            SetupService();
        }

        private void SetupService()
        {

            _service = new SubjectService(_subjectRepositoryMock.Object, _subjectViewUserMap.Object, _professorRepository.Object, _studentRepository.Object);
        }

        private void SetupMock()
        {
            _subjectRepositoryMock = new Mock<ISubjectRepository>();
            _subjectRepositoryMock.Setup(x => x.Add(It.IsAny<Subject>()));

            _subjectRepositoryMock.Setup(x => x.GetByName(It.IsAny<string>())).Returns(new Subject() { Name = "asd" });
        }

        private void SetupData()
        {
            _subjectName = "Name";
            _profesorName = "profesor";
            //throw new NotImplementedException();
        }


        //ScenarionUnderTest_Action_ExpectedResult

        [TestMethod]
        public void AddSubject_SubjectNameIsEmpty_ShouldReturnError()
        {
            //arange
            _subjectName = string.Empty;

            //act /asert
            _service.Add(_subjectName, _profesorName);

            _professorRepository.Verify(x => x.GetByName(It.IsAny<string>()), Times.Once);

            Assert.ThrowsException<ArgumentNullException>( () => _service.Add(_subjectName, _profesorName));
        }

        [TestMethod]
        public void AddSubject_EverytingisFine_ShouldAddSubjectToDB()
        {
            //arange

            //act /asert
            _service.Add(_subjectName, _profesorName);
            _professorRepository.Verify(x => x.GetByName(It.IsAny<string>()), Times.Once);

            _subjectRepositoryMock.Verify(x => x.Add(It.IsAny<Subject>()), Times.Once);
        }
    }
}