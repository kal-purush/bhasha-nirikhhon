using AutoMapper;
using FakeItEasy;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc;
using PCLine_computer_shops.Controllers;
using PCLine_computer_shops.Dtos;
using PCLine_computer_shops.InterfaceReposiotry;
using PCLine_computer_shops.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PCLine_computer_shops.Tests.Controller
{
    public class ShopsControllerTest
    {
        private readonly IShopRepository _shopRepository;
        private readonly IMapper _mapper;


        public ShopsControllerTest()
        {
            _shopRepository = A.Fake<IShopRepository>();
            _mapper = A.Fake<IMapper>();
        }

        [Fact]
        public async Task ShopsController_GetAllShops_ReturnOkAsync()
        {
            var shops = A.Fake<ICollection<ShopGetDto>>();

            var shopsList = A.Fake<List<ShopGetDto>>();

            A.CallTo(() => _mapper.Map<List<ShopGetDto>>(shops)).Returns(shopsList);

            var controller = new ShopsController(_shopRepository, _mapper);

            var result = await controller.GetAllShops();

            result.Should().NotBeNull();

            result.Should().BeOfType(typeof(OkObjectResult)); 
        }

        [Fact]
        public async Task ShopsController_GetShopById_ReturnOkAsync()
        {
            int shopId = 1;
            var shopMap = A.Fake<Shop>();
            var shop = A.Fake<Shop>();
            var shopCreate = A.Fake<ShopCreateDto>();
            var shops = A.Fake<ICollection<ShopGetDto>>();
            var shopsList = A.Fake<List<ShopGetDto>>();

            A.CallTo(() => _mapper.Map<Shop>(shopCreate)).Returns(shop);
            A.CallTo(() => _shopRepository.CreateShop(shop)).Returns(shop);

            var controller = new ShopsController(_shopRepository, _mapper);

            var result = controller.CreateShop(shopCreate);

            result.Should().NotBeNull();
        }
    }
}