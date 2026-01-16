using AutoMapper;
using BL.Services.InterpretationTranslationServices;
using Common.DTOs.InterpretationDTO;
using DAL.Models;
using DAL.Repositories;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Tests.Tests
{
    public class InterpretationTranslationServiceTests
    {
        private readonly InterpretationTranslationService _service;
        private readonly Mock<IUnitOfWork> _unitOfWork = new Mock<IUnitOfWork>();
        private readonly IMapper _mapper = MapperConfig.Initialize();

        private readonly int id = 1;
        private readonly int levelId = 1;
        private readonly int dysfunctionId = 1;
        private readonly int languageId = 1;
        private readonly string text = "This is a test.";

        public InterpretationTranslationServiceTests()
        {
            _service = new InterpretationTranslationService(_unitOfWork.Object, _mapper);
        }

        [Fact]
        public void GetInterpretationTranslationByLevelAndDysfunction_ShouldReturnInterpretationTranslation_WhenIdsAreValid()
        {
            var interpretation = new Interpretation
            {
                Id = id,
                DysfunctionId = dysfunctionId,
                LevelId = levelId
            };

            var interpretationTranslation = new InterpretationTranslation
            {
                LanguageId = languageId,
                InterpretationId = id,
                Text = text,    
                Interpretation = interpretation,              
            };

            var interpretationReadDto = _mapper.Map<InterpretationReadDto>(interpretation);

            _unitOfWork.Setup(x => x.InterpretationRepository.GetInterpretationByLevelAndDysfunction(levelId, dysfunctionId)).Returns(interpretation);
            _unitOfWork.Setup(x => x.InterpretationTranslationRepository.GetInterpretationTranslationByLanguage(id, languageId)).Returns(interpretationTranslation);

            var result = _service.GetInterpretationTranslationByLevelAndDysfunction(languageId, levelId, dysfunctionId);

            Assert.Equal(text, result.Text);
            Assert.Equal(interpretationReadDto.Id, result.Interpretation.Id);
            Assert.Equal(interpretationReadDto.DysfunctionId, result.Interpretation.DysfunctionId);
            Assert.Equal(interpretationReadDto.LevelId, result.Interpretation.LevelId);
        }

        [Fact]
        public void GetInterpretationTranslationByLevelAndDysfunction_ShouldReturnNull_WhenInterpretationDoesNotExists()
        {
            _unitOfWork.Setup(x => x.InterpretationRepository.GetInterpretationByLevelAndDysfunction(It.IsAny<int>(), It.IsAny<int>())).Returns(() => null);

            var result = _service.GetInterpretationTranslationByLevelAndDysfunction(languageId, levelId, dysfunctionId);

            Assert.Null(result);
        }

        [Fact]
        public void GetInterpretationTranslationByLevelAndDysfunction_ShouldReturnNull_WhenInterpretationTranslationDoesNotExists()
        {
            var interpretation = new Interpretation
            {
                Id = id,
                DysfunctionId = dysfunctionId,
                LevelId = levelId
            };

            _unitOfWork.Setup(x => x.InterpretationRepository.GetInterpretationByLevelAndDysfunction(levelId, dysfunctionId)).Returns(interpretation);
            _unitOfWork.Setup(x => x.InterpretationTranslationRepository.GetInterpretationTranslationByLanguage(It.IsAny<int>(), It.IsAny<int>())).Returns(() => null);

            var result = _service.GetInterpretationTranslationByLevelAndDysfunction(languageId, levelId, dysfunctionId);

            Assert.Null(result);
        }
    }
}