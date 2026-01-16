using Microsoft.EntityFrameworkCore;
using Moq;
using System;
using System.Collections.Generic;
using TravelAgency.Controllers;
using TravelAgency.Data;
using TravelAgency.Models;
using Xunit;

namespace TravelAgency.Test
{
	public class TravelApiControllerTest
	{
		[Fact]
		public void ShouldReturnAllJourneys()
		{
			var mockService = new Mock<IApiRequestSend<Journey>>();

			var mockController = new TravelApiController(mockService.Object);
			var data = mockController.GetAllJourneys();

			mockService.Verify(m => m.GetAllData(), Times.Once());
		}

		[Fact]
		public void ShouldDeleteJourney()
		{
			Journey journey = new Journey();
			var mockService = new Mock<IApiRequestSend<Journey>>();

			var mockController = new TravelApiController(mockService.Object);
			mockController.DeleteJourney(journey);

			mockService.Verify(m => m.DeleteEntity(journey), Times.Once());
		}

		[Fact]
		public void ShouldAddNewJourney()
		{
			Journey journey = new Journey();
			var mockService = new Mock<IApiRequestSend<Journey>>();

			var mockController = new TravelApiController(mockService.Object);
			mockController.AddJourney(journey);

			mockService.Verify(m => m.AddEntity(journey), Times.Once());
		}

		[Fact]
		public void ShouldModifyJourney()
		{
			var journey = new Journey();
			journey.Id = 1;

			var mockService = new Mock<IApiRequestSend<Journey>>();

			var controller = new TravelApiController(mockService.Object);
			controller.ModifyJourney(journey.Id, new Journey());

			mockService.Verify(m => m.ModifyEntity(journey.Id, It.Is<Journey>(j => j.Id != journey.Id)), Times.Once());
		}
	}
}