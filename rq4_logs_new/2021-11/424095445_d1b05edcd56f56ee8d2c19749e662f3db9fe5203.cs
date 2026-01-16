using Microsoft.AspNetCore.Mvc;
using Neiria.Api.Controllers;
using Neiria.Domain.Interfaces;
using Neiria.Domain.Models;
using Neiria.Infrastructure.Repositories;
using Neiria.Tests.FakeRepo;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Neiria.Tests
{
  public class Clothes_CRUD_Tests
  {
    private readonly ClothesController _controller;
    private readonly ClothesRepoFake _sut;
    
     
    public Clothes_CRUD_Tests()
    {
      _sut = new ClothesRepoFake();
      _controller = new ClothesController(_sut);

    }

    [Fact]
    public async void Test_Read_GetAllItems_Returns_AllItems()
    {
      // Act
      var result = await _sut.GetAll();

      var total = Assert.IsType<List<Cloth>>(result);

      // Assert

      Assert.Equal(3, total.Count);
    
    }

    [Fact]
    public async void Test_Read_GetId_Return_OkResult()
    {
      // Act

      var clothitem = new Cloth { Guid = new Guid("6299982f-4069-44f6-b88a-840da3ec38ec"), Name = "Vest", BrandName = "C&A", Price = 15 };

      await _sut.Insert(clothitem);

      var getClothId = clothitem.Guid;

      var result = await _sut.GetId(getClothId);

      // Assert
      Assert.NotNull(result);
      Assert.IsType<Cloth>(result);   
    }

    [Fact]
    public async void Test_Create_Insert_ReturnsValid()
    {
      // Act
      var clothitem = new Cloth { Name = "Vest", BrandName = "C&A", Price = 15 };

      var result = await _controller.CreateNewCloth(clothitem); 

      // Assert
      Assert.NotNull(result);
      Assert.IsType<ObjectResult>(result);
      
    }

    [Fact]
    public  async void Test_Delete_Return_NoContentResult()
    {
      // Act
      var item = new Guid("6299982f-4069-44f6-b88a-840da3ec38ec");

     var okRespone = await _controller.DeleteCloth(item);

      // Assert

      Assert.IsType<NoContentResult>(okRespone);
    }

  }
}