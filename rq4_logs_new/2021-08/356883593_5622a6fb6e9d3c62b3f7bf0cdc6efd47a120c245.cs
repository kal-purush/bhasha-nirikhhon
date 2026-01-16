using System;
using System.Collections.Generic;
using System.Net;
using System.Security.Claims;
using System.Threading.Tasks;
using AutoMapper;
using Com.Bateeq.Service.Warehouse.Lib.Facades;
using Com.Bateeq.Service.Warehouse.Lib.Interfaces;
using Com.Bateeq.Service.Warehouse.Lib.Interfaces.PkbjInterfaces;
using Com.Bateeq.Service.Warehouse.Lib.Interfaces.SPKInterfaces;
using Com.Bateeq.Service.Warehouse.Lib.Models.SPKDocsModel;
using Com.Bateeq.Service.Warehouse.Lib.Services;
using Com.Bateeq.Service.Warehouse.Lib.ViewModels.NewIntegrationViewModel;
using Com.Bateeq.Service.Warehouse.Lib.ViewModels.PkbjByUserViewModel;
using Com.Bateeq.Service.Warehouse.Lib.ViewModels.SpkDocsViewModel;
using Com.Bateeq.Service.Warehouse.Test.Helpers;
using Com.Bateeq.Service.Warehouse.WebApi.Controllers.v1.PkpbjControllers;
using Com.Bateeq.Service.Warehouse.WebApi.Controllers.v1.SpkDocsControllers;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Moq;
using Xunit;

namespace Com.Bateeq.Service.Warehouse.Test.Controllers.SPKDocsControllerTests
{
    public class SPKDocsControllerTest
    {
        private Mock<IServiceProvider> GetServiceProvider()
        {
            var serviceProvider = new Mock<IServiceProvider>();
            serviceProvider
                .Setup(x => x.GetService(typeof(IdentityService)))
                .Returns(new IdentityService() { Token = "Token", Username = "Test" });

            serviceProvider
                .Setup(x => x.GetService(typeof(IHttpClientService)))
                .Returns(new HttpClientTestService());

            return serviceProvider;
        }
        private SPKDocstReportController GetController( Mock<ISPKDoc> iSpkdocsFacade)
        {
            var user = new Mock<ClaimsPrincipal>();
            var claims = new Claim[]
            {
                new Claim("username", "unittestusername")
            };
            user.Setup(u => u.Claims).Returns(claims);

            var servicePMock = GetServiceProvider();

            SPKDocstReportController controller = new SPKDocstReportController(null, iSpkdocsFacade.Object, servicePMock.Object)
            {
                ControllerContext = new ControllerContext()
                {
                    HttpContext = new DefaultHttpContext()
                    {
                        User = user.Object
                    }
                }
            };
            controller.ControllerContext.HttpContext.Request.Headers["Authorization"] = "Bearer unittesttoken";
            controller.ControllerContext.HttpContext.Request.Path = new PathString("/v1/unit-test");
            controller.ControllerContext.HttpContext.Request.Headers["x-timezone-offset"] = "7";

            return controller;
        }
        
        private SPKDocsFromFinihsingOutsViewModel ViewModel
        {
            get
            {
                return new SPKDocsFromFinihsingOutsViewModel
                {
                    FinishingOutDate = DateTimeOffset.Now,
                    UnitTo = new DestinationViewModel
                    {
                        _id = 1,
                        code = "code",
                        name = "name"
                    },
                    Unit = new SourceViewModel
                    {
                        code = "code",
                        name = "name",
                        _id = 1
                    },
                    PackingList = "0001/FER/08/21",
                    Password = "pass",
                    IsDifferentSize = false,
                    Weight = 0,
                    Comodity = new Comodity()
                    {
                        code = "code",
                        name = "name",
                        id = 1
                    },
                    Items = new List<SPKDocItemsFromFinihsingOutsViewModel>
                    {
                        new SPKDocItemsFromFinihsingOutsViewModel
                        {
                            IsDifferentSize = false
                        }
                    }
                };
            }
        }
        
        protected int GetStatusCode(IActionResult response)
        {
            return (int)response.GetType().GetProperty("StatusCode").GetValue(response, null);
        }
        
        [Fact]
        public async Task Should_Success_Create_Data()
        {
            var mockFacade = new Mock<ISPKDoc>();
            mockFacade.Setup(x => x.Create(It.IsAny<SPKDocsFromFinihsingOutsViewModel>(), It.IsAny<string>(), It.IsAny<string>()))
                .ReturnsAsync(1);

            var controller = GetController(mockFacade);

            var response = await controller.Post(this.ViewModel);
            Assert.Equal((int)HttpStatusCode.Created, GetStatusCode(response));
        }
        
        [Fact]
        public async Task Should_Error_Create_Data()
        {
            var mockFacade = new Mock<ISPKDoc>();
            mockFacade.Setup(x => x.Create(It.IsAny<SPKDocsFromFinihsingOutsViewModel>(), It.IsAny<string>(), It.IsAny<string>()))
                .ThrowsAsync(new Exception());

            var controller = GetController(mockFacade);

            var response = await controller.Post(this.ViewModel);
            Assert.Equal((int)HttpStatusCode.InternalServerError, GetStatusCode(response));
        }
    }
}