using AskMe.Services.Readers;
using AskMe.Services.Sets;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Moq;
using AskMe.Repositories.Sets;
using AskMe.Services.Formaters;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http;
using AskMe.Data.Models.Sets;
using AskMe.Data.Models.Utils;

namespace TestAskMe.ServiceTests
{
    public class TestSetService
    {
        private ISetService _setService;
        private Mock<ITxtReader> _mockTxtReader;
        private Mock<ITxtFormater> _mockTxtFormater;
        private Mock<ISetRepository> _mockSetRepository;
        private Mock<ILogger<SetService>> _mockLogger;
        const string _validInputString = "##Theme1\n#Question1\n-Answer1";
        MemoryStream _stream;
        FormFile _file;

        [SetUp]
        public void Setup()
        {
            _mockTxtReader = new Mock<ITxtReader>();
            _mockTxtFormater = new Mock<ITxtFormater>();
            _mockSetRepository = new Mock<ISetRepository>();
            _mockLogger = new Mock<ILogger<SetService>>();

            _setService = new SetService(
                _mockTxtReader.Object,
                _mockTxtFormater.Object,
                _mockSetRepository.Object,
                _mockLogger.Object
            );

            _stream = new MemoryStream(Encoding.UTF8.GetBytes(_validInputString));
            _file = new FormFile(_stream, 0, _stream.Length, "file", "test.txt")
            {
                Headers = new HeaderDictionary(),
                ContentType = "text/plain"
            };
        }

        [Test]
        public async Task TestCreateFormatedSetPreviewFromValidInput()
        {
            var req = new SetRequest("TestSet", "TestDescription", _file);
            var userId = "TestUserId";
            List<Line> lines = new List<Line> {
                new Line(true, false, "Theme1"),
                new Line(false, true, "Question1"),
                new Line(false, false, "Answer1")
            };

            _mockTxtReader.Setup(x => x.ReadTxtByLines(_file)).ReturnsAsync(lines);

            var result = await _setService.CreateFormatedSetPreview(req, userId);

            Assert.That(result.Themes.Count, Is.EqualTo(2));   //Default + Theme1
            Assert.That(result.Themes[0].Name, Is.EqualTo("Default"));
            Assert.That(result.Themes[1].Name, Is.EqualTo("Theme1"));
            Assert.That(result.Themes[1].Questions.Count, Is.EqualTo(1));
            Assert.That(result.Themes[1].Questions[0].Text, Is.EqualTo("Question1"));
            Assert.That(result.Themes[1].Questions[0].Answers.Count, Is.EqualTo(1));
            Assert.That(result.Themes[1].Questions[0].Answers[0].Text, Is.EqualTo("Answer1"));
        }

    }
}