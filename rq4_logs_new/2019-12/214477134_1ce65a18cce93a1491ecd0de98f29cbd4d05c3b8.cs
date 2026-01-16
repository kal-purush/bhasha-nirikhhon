using AutoMapper;
using CentralDeErros.Application.Mapping;
using CentralDeErros.Application.Services;
using CentralDeErros.Data.Context;
using CentralDeErros.Data.Repositories;
using CentralDeErros.Domain.Interfaces;
using CentralDeErros.Domain.Models;
using CentralDeErros.Domain.Services;
using Microsoft.EntityFrameworkCore;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace CentralDeErros.Application.Test.Services
{
    public class ErrorLogAppServiceTest
    {
        private readonly IErrorLogRepository _repository;
        protected readonly IMapper _mapper;
        private List<ErrorLog> fakeData = new List<ErrorLog>();
        private Mock<CentralDeErrosContext> _fakeContext;
        public ErrorLogAppServiceTest()
        {
            fakeData.AddRange(new List<ErrorLog>() {
                new ErrorLog() { Id = Guid.NewGuid(), Message = "Error Message 1", Archieved = false },
                new ErrorLog() { Id = Guid.NewGuid(), Message = "Error Message 2", Archieved = false },
                new ErrorLog() { Id = Guid.NewGuid(), Message = "Error Message 3", Archieved = false },
            });

            var fakeErrorLogs = fakeData.AsQueryable();
            var fakeDbSet = new Mock<DbSet<ErrorLog>>();
            fakeDbSet.As<IQueryable<ErrorLog>>().Setup(x => x.Provider).Returns(fakeErrorLogs.Provider);
            fakeDbSet.As<IQueryable<ErrorLog>>().Setup(x => x.Expression).Returns(fakeErrorLogs.Expression);
            fakeDbSet.As<IQueryable<ErrorLog>>().Setup(x => x.ElementType).Returns(fakeErrorLogs.ElementType);
            fakeDbSet.As<IQueryable<ErrorLog>>().Setup(x => x.GetEnumerator()).Returns(fakeErrorLogs.GetEnumerator()); 

            _fakeContext = new Mock<CentralDeErrosContext>();
            _fakeContext.Setup(m => m.Set<ErrorLog>()).Returns(fakeDbSet.Object);

            _repository = new ErrorLogRepository(_fakeContext.Object);

            _mapper = new Mapper(
            new MapperConfiguration(
                configure => {
                        configure.AddProfile<AutoMappingDomainToViewModel>();
                        configure.AddProfile<AutoMappingViewModelToDomain>();
                    }
            )
        );
        }

        [Fact]
        public void Should_Return_All_ErrorLogs()
        {
            var service = new ErrorLogService(_repository);
            var appService = new ErrorLogAppService(service, _mapper);

            var actual = appService.GetAll();
            Assert.NotNull(actual);
            Assert.Equal(fakeData.Count(), actual.Count());
        }

        [Fact]
        public void Should_Archieve_ErrorLog()
        {
            var service = new ErrorLogService(_repository);
            var appService = new ErrorLogAppService(service, _mapper);
            var actual = appService.Find(a => a.Id == fakeData[0].Id).FirstOrDefault();

            Assert.NotNull(actual);
            Assert.False(actual.Archieved);

            var changed = appService.ArchieveErrorLog(actual.Id);
            
            Assert.NotNull(changed);
            Assert.True(changed.Archieved);
        }
    }

}