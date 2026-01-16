using AutoMapper;
using CarService.RepariOrders.Api.Models.Responses;
using CarService.RepariOrders.Api.MapperProfiles;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using CarService.RepariOrders.Api.Models.Domain;

namespace CarService.Tests.Mappers
{
    public class RepairOrderMapperTest
    {
        private readonly IMapper _repairOrderMapper;

        public RepairOrderMapperTest()
        {
            var config = new MapperConfiguration(cfg => cfg.AddProfile<RepairOrderMapperProfile>());
            _repairOrderMapper = config.CreateMapper();
        }

        [Fact]
        public void RepairOrder_Mapper_Configuration_IsValid()
        {
            // Arrange
            var config = new MapperConfiguration(cfg => cfg.AddProfile<RepairOrderMapperProfile>());

            // Assert
            config.AssertConfigurationIsValid();
        }

        [Fact]
        public void RepairOrder_Mapper_Correct_Mapping()
        {
            // Arrange
            var repairOrder = new RepairOrder
            {
                Id = Guid.NewGuid(),
                CarId = Guid.NewGuid(),
                OrderDate = DateTime.Now,
                Price = 100
            };

            // Act
            var repairOrderResponse = _repairOrderMapper.Map<RepairOrderResponse>(repairOrder);

            // Assert
            Assert.True(repairOrderResponse.Id == repairOrder.Id && repairOrderResponse.CarId == repairOrder.CarId
                && repairOrderResponse.OrderDate == repairOrder.OrderDate && repairOrderResponse.Price == repairOrder.Price);
        }
    }
}