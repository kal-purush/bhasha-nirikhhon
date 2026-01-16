using System;
using System.Collections.Generic;
using System.Text;
using Studio_Inventory_API.Controllers;
using Studio_Inventory_API.Repositories;
using Studio_Inventory_API.Models;
using NSubstitute;
using Xunit;

namespace Studio_Inventory_API.Tests
{
    public class EquipmentListControllerTests
    {
        EquipmentListController sut;
        IRepository<Equipment> equipmentRepo;

        public EquipmentListControllerTests()
        {
            equipmentRepo = Substitute.For<IRepository<Equipment>>();
            sut = new EquipmentListController(equipmentRepo);
        }

        [Fact]
        public void Get_Equipment_List_Returns_A_List()
        {
            IEnumerable<Equipment> expectedList = new List<Equipment>();
            equipmentRepo.GetAll().Returns(expectedList);
            var result = sut.GetEquipmentList();
            Assert.Equal(expectedList, result);
        }

        [Fact]
        public void Get_Equipment_Returns_An_Equipment()
        {
            var expectedAlbum = new Equipment();
            equipmentRepo.GetById(1).Returns(expectedAlbum);
            var result = sut.GetEquipment(1);
            Assert.Equal(expectedAlbum, result);
        }
    }
}