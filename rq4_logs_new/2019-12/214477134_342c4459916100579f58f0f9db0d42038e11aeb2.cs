using AutoMapper;
using CentralDeErros.Application.Mapping;
using CentralDeErros.Application.Services;
using CentralDeErros.Data.Context;
using CentralDeErros.Data.Repositories;
using CentralDeErros.Domain.Interfaces;
using CentralDeErros.Domain.Interfaces.Repositories;
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
    public class UserAppServiceTest
    {
        private readonly IUserRepository _repository;
        protected readonly IMapper _mapper;
        private List<User> fakeData = new List<User>();
        private Mock<CentralDeErrosContext> _fakeContext;
        public UserAppServiceTest()
        {
            fakeData.AddRange(new List<User>() {
                new User() { Id = Guid.NewGuid(), Name= "Jonathan Guths", Active = true },
                new User() { Id = Guid.NewGuid(), Name= "Joao Silva", Active = true },
            });

            var fakeErrorLogs = fakeData.AsQueryable();
            var fakeDbSet = new Mock<DbSet<User>>();
            fakeDbSet.As<IQueryable<User>>().Setup(x => x.Provider).Returns(fakeErrorLogs.Provider);
            fakeDbSet.As<IQueryable<User>>().Setup(x => x.Expression).Returns(fakeErrorLogs.Expression);
            fakeDbSet.As<IQueryable<User>>().Setup(x => x.ElementType).Returns(fakeErrorLogs.ElementType);
            fakeDbSet.As<IQueryable<User>>().Setup(x => x.GetEnumerator()).Returns(fakeErrorLogs.GetEnumerator());

            _fakeContext = new Mock<CentralDeErrosContext>();
            _fakeContext.Setup(m => m.Set<User>()).Returns(fakeDbSet.Object);

            _repository = new UserRepository(_fakeContext.Object);

            _mapper = new Mapper(
            new MapperConfiguration(
                configure =>
                {
                    configure.AddProfile<AutoMappingDomainToViewModel>();
                    configure.AddProfile<AutoMappingViewModelToDomain>();
                }
            )
        );
        }


        [Fact]
        public void Should_Get_By_Id()
        {
            var service = new UserService(_repository);
            var appService = new UserAppService(service, _mapper);
            var actual = appService.Find(a => a.Id == fakeData[0].Id).FirstOrDefault();
            Assert.NotNull(actual);
            
            var changed = appService.GetById(actual.Id);
            Assert.NotNull(changed);
        }

    }
}