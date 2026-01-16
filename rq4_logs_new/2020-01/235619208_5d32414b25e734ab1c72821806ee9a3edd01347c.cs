using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ViewFeatures;
using Microsoft.Extensions.Configuration;
using Moq;
using Newtonsoft.Json;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using VRTestWeb.Razor.Controllers;
using VRTestWeb.Razor.Models;
using VRTestWeb.Razor.Services;

namespace VRTest.UnitTest.VRTestWeb_Razor
{
    [TestFixture]
    public class NPVControllerTest
    {
        private Mock<IHttpService> _httpServiceMock;
        private Mock<IConfiguration> _configurationMock;
        

        [SetUp]
        public void Setup()
        {
            _httpServiceMock = new Mock<IHttpService>(MockBehavior.Strict);
            _configurationMock = new Mock<IConfiguration>(MockBehavior.Strict);

           
        }

        [TestCase("1000, 2000, 3000, 4000, 5000", .25, 1.5, 1.0, 1000, "13463.71,13333.81,13205.33")]
        public void Compute_Given_Request_Has_Not_Been_Made_Before_Returns_Object_From_Database(
            string cashFlowDescription
            , double increment
            , double upperBoundDiscountRate
            , double lowerBoundDiscountRate
            , double initialCost
            , string expectedNPVs
            )
        {
            var npvModel = new NPVModel
            {
                CashFlowsDescription = cashFlowDescription
                ,
                Increment = increment
                ,
                InitialCost = initialCost
                ,
                LowerBoundDiscountRate = lowerBoundDiscountRate
                ,
                UpperBoundDiscountRate = upperBoundDiscountRate
            };
            var listOfExpectedNPVs = expectedNPVs.Split(",").Select(n => double.Parse(n)).ToList();
            var listOfNPVDetailModel = new List<NPVDetailModel>();

            var discountRate = lowerBoundDiscountRate;
            for (var i = 0; i < listOfExpectedNPVs.Count; i++)
            {
                listOfNPVDetailModel.Add(new NPVDetailModel
                {
                    CashFlowSummary = cashFlowDescription
                    ,
                    DiscountRate = discountRate
                    ,InitialCost = initialCost
                    ,NPV = listOfExpectedNPVs[i]
                    
                });
                discountRate += increment;
            }

            var listOfNPVDetailModelStr = JsonConvert.SerializeObject(listOfNPVDetailModel);

            _httpServiceMock.Setup(h => h.GetAsJson<NPVRequestModel>(It.IsAny<string>(), It.IsAny<NPVRequestModel>()))
                .ReturnsAsync(string.Empty);
            _httpServiceMock.Setup(h => h.PostAsyncReturnAsJson(It.IsAny<string>(), It.IsAny<NPVRequestModel>()))
                .ReturnsAsync(listOfNPVDetailModelStr);
            _configurationMock.SetupGet(c => c[It.IsAny<string>()]).Returns("http://localhost:63620/api/npv");
            var httpContext = new DefaultHttpContext();
            var tempData = new TempDataDictionary(httpContext, Mock.Of<ITempDataProvider>());
            

            var controller = new VRTestWeb.Razor.Controllers.NPVController(
               _configurationMock.Object
               , _httpServiceMock.Object
               )
            { TempData = tempData};
            
            var response = controller.Compute(npvModel).Result;
            var redirectToActionResult = response as RedirectToActionResult;
            Assert.IsNotNull(redirectToActionResult);
            Assert.AreEqual("Index", redirectToActionResult.ActionName);

            _httpServiceMock.VerifyAll();
            _configurationMock.VerifyAll();

        }


        [TestCase("1000, 2000, 3000, 4000, 5000", .25, 1.5, 1.0, 1000, "13463.71,13333.81,13205.33")]
        public void Compute_Given_Request_Has_Been_Made_Before_Returns_Computed_Object(
            string cashFlowDescription
            , double increment
            , double upperBoundDiscountRate
            , double lowerBoundDiscountRate
            , double initialCost
            , string expectedNPVs
            )
        {
            var npvModel = new NPVModel
            {
                CashFlowsDescription = cashFlowDescription
                ,
                Increment = increment
                ,
                InitialCost = initialCost
                ,
                LowerBoundDiscountRate = lowerBoundDiscountRate
                ,
                UpperBoundDiscountRate = upperBoundDiscountRate
            };
            var listOfExpectedNPVs = expectedNPVs.Split(",").Select(n => double.Parse(n)).ToList();
            var listOfNPVDetailModel = new List<NPVDetailModel>();

            var discountRate = lowerBoundDiscountRate;
            for (var i = 0; i < listOfExpectedNPVs.Count; i++)
            {
                listOfNPVDetailModel.Add(new NPVDetailModel
                {
                    CashFlowSummary = cashFlowDescription
                    ,
                    DiscountRate = discountRate
                    ,
                    InitialCost = initialCost
                    ,
                    NPV = listOfExpectedNPVs[i]

                });
                discountRate += increment;
            }

            var listOfNPVDetailModelStr = JsonConvert.SerializeObject(listOfNPVDetailModel);

            _httpServiceMock.Setup(h => h.GetAsJson<NPVRequestModel>(It.IsAny<string>(),It.IsAny<NPVRequestModel>()))
                .ReturnsAsync(listOfNPVDetailModelStr);
          
            _configurationMock.SetupGet(c => c[It.IsAny<string>()]).Returns("http://localhost:63620/api/npv");
            var httpContext = new DefaultHttpContext();
            var tempData = new TempDataDictionary(httpContext, Mock.Of<ITempDataProvider>());


            var controller = new VRTestWeb.Razor.Controllers.NPVController(
               _configurationMock.Object
               , _httpServiceMock.Object
               )
            { TempData = tempData };

            var response = controller.Compute(npvModel).Result;
            var redirectToActionResult = response as RedirectToActionResult;
            Assert.IsNotNull(redirectToActionResult);
            Assert.AreEqual("Index", redirectToActionResult.ActionName);

            _httpServiceMock.VerifyAll();
            _configurationMock.VerifyAll();

        }
    }
}