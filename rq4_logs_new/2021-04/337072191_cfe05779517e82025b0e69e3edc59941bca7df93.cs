using AutoMapper;
using BL.Services.IndividualScoreServices;
using Common.DTOs.AnswerDTO;
using Common.DTOs.IndividualScoreDTO;
using DAL.Models;
using DAL.Repositories;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Tests.Tests
{
    public class IndividualScoreServiceTests
    {
        private readonly IndividualScoreService _service;
        private readonly Mock<IUnitOfWork> _unitOfWork = new Mock<IUnitOfWork>();
        private readonly IMapper _mapper = MapperConfig.Initialize();

        private int teamMemberId = 1;
        private int teamscanId = 1;
        private readonly decimal scoreTrust = 3.5m;
        private readonly decimal scoreAccountability = 3.5m;
        private readonly decimal scoreCommitment = 2.5m;
        private readonly decimal scoreConflict = 3.5m;
        private readonly decimal scoreResults = 3m;

        private readonly List<AnswerReadDto> list = new List<AnswerReadDto>()
        {
            new AnswerReadDto() { DysfunctionId = 1, Score = 2 },
            new AnswerReadDto() { DysfunctionId = 1, Score = 5 },
            new AnswerReadDto() { DysfunctionId = 2, Score = 3 },
            new AnswerReadDto() { DysfunctionId = 2, Score = 4 },
            new AnswerReadDto() { DysfunctionId = 3, Score = 1 },
            new AnswerReadDto() { DysfunctionId = 3, Score = 4 },
            new AnswerReadDto() { DysfunctionId = 4, Score = 3 },
            new AnswerReadDto() { DysfunctionId = 4, Score = 4 },
            new AnswerReadDto() { DysfunctionId = 5, Score = 5 },
            new AnswerReadDto() { DysfunctionId = 5, Score = 1 },
        };

        public IndividualScoreServiceTests()
        {
            _service = new IndividualScoreService(_unitOfWork.Object, _mapper);
        }

        [Fact]
        public void GetIndividualScoreByIdIncludingTeamscan_ShouldReturnIndividualScore_WhenIndividualScoreExists()
        {
            var id = Guid.NewGuid();

            var individualScore = new IndividualScore
            {
                Id = id,
                TeamMemberId = teamMemberId,
                TeamscanId = teamscanId,
                ScoreTrust = 0,
                ScoreAccountability = 0,
                ScoreCommitment = 0,
                ScoreConflict = 0,
                ScoreResults = 0,
                HasAnswered = false
            };

            _unitOfWork.Setup(x => x.IndividualScoreRepository.GetIndividualScoreByIdIncludingTeamscan(id)).Returns(individualScore);

            var result = _service.GetIndividualScoreByIdIncludingTeamscan(id);

            Assert.Equal(id, result.Id);
            Assert.Equal(teamMemberId, result.TeamMemberId);
            Assert.Equal(teamscanId, result.TeamscanId);
        }

        [Fact]
        public void GetIndividualScoreByIdIncludingTeamscan_ShouldReturnNull_WhenIndividualScoreDoesNotExists()
        {
            _unitOfWork.Setup(x => x.IndividualScoreRepository.GetIndividualScoreByIdIncludingTeamscan(It.IsAny<Guid>())).Returns(() => null);

            var result = _service.GetIndividualScoreByIdIncludingTeamscan(Guid.NewGuid());

            Assert.Null(result);
        }

        [Fact]
        public void GetIndividualScoreById_ShouldReturnIndividualScore_WhenIndividualScoreExists()
        {
            var id = Guid.NewGuid();

            var individualScore = new IndividualScore
            {
                Id = id,
                TeamMemberId = teamMemberId,
                TeamscanId = teamscanId,
                ScoreTrust = 0,
                ScoreAccountability = 0,
                ScoreCommitment = 0,
                ScoreConflict = 0,
                ScoreResults = 0,
                HasAnswered = false
            };

            _unitOfWork.Setup(x => x.IndividualScoreRepository.GetIndividualScoreById(id)).Returns(individualScore);

            var result = _service.GetIndividualScoreById(id);

            Assert.Equal(id, result.Id);
            Assert.Equal(teamMemberId, result.TeamMemberId);
            Assert.Equal(teamscanId, result.TeamscanId);
        }

        [Fact]
        public void GetIndividualScoreById_ShouldReturnNull_WhenIndividualScoreDoesNotExists()
        {
            _unitOfWork.Setup(x => x.IndividualScoreRepository.GetIndividualScoreById(It.IsAny<Guid>())).Returns(() => null);

            var result = _service.GetIndividualScoreById(Guid.NewGuid());

            Assert.Null(result);
        }

        [Fact]
        public void CalculateIndividualScore_ShouldReturnCalculatedIndividualScore()
        {
            var id = Guid.NewGuid();

            var result = _service.CalculateIndividualScore(id, list);

            Assert.Equal(id, result.Id);
            Assert.Equal(scoreTrust, result.ScoreTrust);
            Assert.Equal(scoreAccountability, result.ScoreAccountability);
            Assert.Equal(scoreCommitment, result.ScoreCommitment);
            Assert.Equal(scoreConflict, result.ScoreConflict);
            Assert.Equal(scoreResults, result.ScoreResults);
            Assert.True(result.HasAnswered);
        }  
        
        [Fact]
        public void CalculateTeamscore_ShouldReturnCalculatedTeamscan()
        {
            var id = Guid.NewGuid();

            var updatedScore = new IndividualScore()
            {
                Id = id,
                TeamscanId = teamscanId,
                TeamMemberId = teamMemberId,
                ScoreTrust = scoreTrust,
                ScoreAccountability = scoreAccountability,
                ScoreCommitment = scoreCommitment,
                ScoreConflict = scoreConflict,
                ScoreResults = scoreResults,
                HasAnswered = true,
            };

            _unitOfWork.Setup(x => x.IndividualScoreRepository.GetAllAnsweredByTeamscan(teamscanId)).Returns(Enumerable.Empty<IndividualScore>());

            var result = _service.CalculateTeamscore(teamscanId, updatedScore, true);

            Assert.Equal(teamscanId, result.Id);
            Assert.Equal(scoreTrust, result.ScoreTrust);
            Assert.Equal(scoreAccountability, result.ScoreAccountability);
            Assert.Equal(scoreCommitment, result.ScoreCommitment);
            Assert.Equal(scoreConflict, result.ScoreConflict);
            Assert.Equal(scoreResults, result.ScoreResults);
            Assert.Equal(DateTime.Today, result.EndDate);
        }
    }
}