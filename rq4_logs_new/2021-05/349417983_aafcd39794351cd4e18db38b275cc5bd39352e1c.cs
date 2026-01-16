using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using WebStore.Controllers;
using WebStore.Domain.DTO.Orders;
using WebStore.Domain.DTO.Products;
using WebStore.Domain.Entities;
using WebStore.Domain.Identity;
using WebStore.Domain.ViewModels;
using WebStore.Interfaces.Services;
using Assert = Xunit.Assert;

namespace WebStore.UI.Tests.Controllers
{
    [TestClass]
    public class CartControllerTests // 1:43
    {
        #region First attempt

        //[TestMethod]
        //public async Task Checkout_ModelState_Invalid_Returns_View_with_Model2()
        //{
        //    //Arrange
        //    var mock = new Mock<ICartService>();

        //    var cartItems = new List<(ProductViewModel product, int quantity)>() { 
        //        (new ProductViewModel() { Id = 1, Name = "Product_1", Brand = "Brand_1", Price = 11m, ImageUrl = "img1.png" }, 1),
        //        (new ProductViewModel() { Id = 2, Name = "Product_2", Brand = "Brand_2", Price = 22m, ImageUrl = "img2.png" }, 1),
        //        (new ProductViewModel() { Id = 3, Name = "Product_3", Brand = "Brand_3", Price = 33m, ImageUrl = "img3.png" }, 1)
        //    };

        //    mock.Setup(x => x.TransformToViewModel()).Returns(() => new CartViewModel() { Items = cartItems });

        //    CartController controller = new CartController(mock.Object, null);

        //    controller.ModelState.AddModelError("testError", $"Ошибка сгенерированная в тестовом методе " +
        //        $"{nameof(Checkout_ModelState_Invalid_Returns_View_with_Model)} тестового класса {nameof(CartControllerTests)} " +
        //        $"для тестирования первого условного оператора метода {nameof(controller.Checkout)} контроллера {nameof(CartController)}");

        //    OrderViewModel orderViewModel = new OrderViewModel() { 
        //        Id = 42, UserId = "112233", Address = "TestAdress", Phone = "7777777", Date = DateTime.Now
        //    };

        //    //Act
        //    var result = await controller.Checkout(orderViewModel, null);

        //    //Assert
        //    var viewResult = Assert.IsType<ViewResult>(result);
        //    var model = Assert.IsType<CartOrderViewModel>(viewResult.Model);
        //    Assert.Equal(cartItems.Count, model.Cart.ItemsCount());

        //    for (int i = 0; i < cartItems.Count; i++) {
        //        Assert.Equal(cartItems.ElementAt(i).product.Id, model.Cart.Items.ToList().ElementAt(i).product.Id);
        //        Assert.Equal(cartItems.ElementAt(i).product.Name, model.Cart.Items.ToList().ElementAt(i).product.Name);
        //        Assert.Equal(cartItems.ElementAt(i).product.Brand, model.Cart.Items.ToList().ElementAt(i).product.Brand);
        //        Assert.Equal(cartItems.ElementAt(i).product.Price, model.Cart.Items.ToList().ElementAt(i).product.Price);
        //    }
            
        //    Assert.Equal(orderViewModel, model.Order);
        //}
        #endregion        

        [TestMethod]
        public async Task Checkout_ModelState_Invalid_Returns_View_with_Model()
        {
            //Arrange
            Mock<ICartService> mockCartService = new Mock<ICartService>();
            UserManager<User> mockUserManager = TestUserManager<User>();

            CartController controller = new CartController(mockCartService.Object, mockUserManager);
            controller.ModelState.AddModelError("testError", $"Ошибка сгенерированная в тестовом методе " +
                $"{nameof(Checkout_ModelState_Invalid_Returns_View_with_Model)} тестового класса {nameof(CartControllerTests)} " +
                $"для тестирования первого условного оператора метода {nameof(controller.Checkout)} контроллера {nameof(CartController)}");

            int expectedOrderId = 11223344;
            OrderViewModel orderViewModel = new OrderViewModel() { Id = expectedOrderId };

            Mock<IOrderService> mockOrderService = new Mock<IOrderService>();

            //Act
            IActionResult ret_IActionResult = await controller.Checkout(orderViewModel, mockOrderService.Object);

            //Assert
            ViewResult ret_ViewResult = Assert.IsType<ViewResult>(ret_IActionResult);
            CartOrderViewModel model = Assert.IsAssignableFrom<CartOrderViewModel>(ret_ViewResult.Model);
            Assert.Equal(expectedOrderId, model.Order.Id);
        }

        [TestMethod]
        public async Task Checkout_Calls_Service_And_Returns_Redirect() {

            //Arrange

            Mock<ICartService> mockCartService = new Mock<ICartService>();

            var cartItems = new List<(ProductViewModel product, int quantity)>() {
                (new ProductViewModel() { Id = 1, Name = "Product_1", Brand = "Brand_1", Price = 11m, ImageUrl = "img1.png" }, 1),
                (new ProductViewModel() { Id = 2, Name = "Product_2", Brand = "Brand_2", Price = 22m, ImageUrl = "img2.png" }, 1),
                (new ProductViewModel() { Id = 3, Name = "Product_3", Brand = "Brand_3", Price = 33m, ImageUrl = "img3.png" }, 1)
            };

            mockCartService.Setup(x => x.TransformToViewModel()).Returns(() => new CartViewModel() { Items = cartItems });

            Mock<IOrderService> mockOrderService = new Mock<IOrderService>();

            List<OrderItemDTO> orderItemDTOs = new List<OrderItemDTO>(){ 
                new OrderItemDTO(1, new ProductDTO(1, "TestProduct_1", 1, 11m, "img1.url", null, null), 11m, 1),
                new OrderItemDTO(2, new ProductDTO(2, "TestProduct_2", 2, 22m, "img2.url", null, null), 22m, 1)
            };

            int expectedValue = 36984;
            OrderDTO orderDTO = new OrderDTO(expectedValue, new User(), "TestAdress", "TestPhone", DateTime.Now, orderItemDTOs);
            mockOrderService.Setup(x => x.CreateOrder(It.IsAny<CreateOrderModel>())).ReturnsAsync(() => orderDTO);

            OrderViewModel orderViewModel = new OrderViewModel() 
            { Id = 11223344, UserId = "78134", Address = "TestAdress", Date = DateTime.Now, Phone = "TestPhone" };

            UserManager<User> mockUserManger = TestUserManager<User>();

            CartController cartController = new CartController(mockCartService.Object, mockUserManger);

            //Act

            IActionResult result = await cartController.Checkout(orderViewModel, mockOrderService.Object);

            //Assert

            RedirectToActionResult ret_RedirectToActionResult = Assert.IsType<RedirectToActionResult>(result); //Not derived type (why view result)
            Assert.Equal(nameof(cartController.OrderConfirmed), ret_RedirectToActionResult.ActionName);
            Assert.Null(ret_RedirectToActionResult.ControllerName);
            Assert.Equal(expectedValue, ret_RedirectToActionResult.RouteValues["id"]);
        }

        public static UserManager<TUser> TestUserManager<TUser>(IUserStore<TUser> store = null) where TUser : class
        {
            store = store ?? new Mock<IUserStore<TUser>>().Object;
            var options = new Mock<IOptions<IdentityOptions>>();
            var idOptions = new IdentityOptions();
            idOptions.Lockout.AllowedForNewUsers = false;
            options.Setup(o => o.Value).Returns(idOptions);
            var userValidators = new List<IUserValidator<TUser>>();
            var validator = new Mock<IUserValidator<TUser>>();
            userValidators.Add(validator.Object);
            var pwdValidators = new List<PasswordValidator<TUser>>();
            pwdValidators.Add(new PasswordValidator<TUser>());
            var userManager = new UserManager<TUser>(store, options.Object, new PasswordHasher<TUser>(),
                userValidators, pwdValidators, new UpperInvariantLookupNormalizer(),
                new IdentityErrorDescriber(), null,
                new Mock<ILogger<UserManager<TUser>>>().Object);
            validator.Setup(v => v.ValidateAsync(userManager, It.IsAny<TUser>()))
                .Returns(Task.FromResult(IdentityResult.Success)).Verifiable();

            return userManager;
        }
    }
}