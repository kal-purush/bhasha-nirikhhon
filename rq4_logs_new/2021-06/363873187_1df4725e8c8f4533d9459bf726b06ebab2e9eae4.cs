using AutoMapper;
using CSB.Business.Enums;
using CSB.Business.Exceptions;
using CSB.Business.Impl;
using CSB.Repository.Interfaces;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace CSB.Business.UnitTests
{
    public class EmployeeServiceTests
    {/*public async Task<GetEmployeeDto> GetByIdAsync(int id)
        {
            if (id <= 0)
            {
                throw new ArgumentNullException(nameof(id));
            }

            var employee = await _employeeRepository.GetByIdAsync(id);
            if (employee is null)
            {
                throw new NotFoundException(nameof(Employee));
            }

            return _mapper.Map<GetEmployeeDto>(employee);
        }*/

        private readonly IMapper _mapper;

        public EmployeeServiceTests()
        {
            _mapper = new MapperConfiguration(x => x.AddProfile<BusinessProfile>()).CreateMapper();
        }

        [Fact]
        public async Task GetByIdAsync_throws_ArgumentNullException()
        {
            var employeeService = new EmployeeService(_mapper, null);
            await Assert.ThrowsAsync<ArgumentNullException>(() => employeeService.GetByIdAsync(0));
        }

        [Fact]
        public async Task GetByIdAsync_throws_NotFoundException()
        {
            var repositoryMock = new Mock<IEmployeeRepository>();
            var employeeService = new EmployeeService(_mapper, repositoryMock.Object);
            await Assert.ThrowsAsync<NotFoundException>(() => employeeService.GetByIdAsync(1));
        }

        [Fact]
        public async Task GetByIdAsync_SuccessAsync()
        {
            var repositoryMock = new Mock<IEmployeeRepository>();
            repositoryMock.Setup(x => x.GetByIdAsync(It.IsAny<int>()))
                .ReturnsAsync(new Repository.Entities.Employee
                {
                    DateOfBirth = DateTime.Today.AddYears(-18),
                    Email = "mail@mail.com",
                    FirstName = "First",
                    LastName = "Last",
                    Gender = 1,
                    Position = null,
                    Status = 1
                });
            var employeeService = new EmployeeService(_mapper, repositoryMock.Object);
            var entity = await employeeService.GetByIdAsync(1);

            Assert.Equal(DateTime.Today.AddYears(-18), entity.DateOfBirth);
            Assert.Equal("mail@mail.com", entity.Email);
            Assert.Equal("First", entity.FirstName);
            Assert.Equal("Last", entity.LastName);
            Assert.Equal(Gender.Male, entity.Gender);
            Assert.Equal(EmployeeStatus.Active, entity.Status);
        }
    }
}