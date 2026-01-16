using DFC.App.MatchSkills.Interfaces;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;

namespace DFC.App.MatchSkills.Test.Service
{
    public class FileServiceTests
    {
        [Test]
        public void When_ReadAllText_ReturnTextFromFile()
        {
             var _fileService = Substitute.For<IFileService>();
            
             var results = _fileService.ReadAllText("").ReturnsForAnyArgs("My File Content");
             
            results.Should().Be("My File Content");
            
        }
    }
}