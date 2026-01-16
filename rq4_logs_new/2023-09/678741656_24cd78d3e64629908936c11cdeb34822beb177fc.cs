using System;
using Application.ParkingSlots.Queries.GetFilteredParkingSlots;
using Domain.Entities;
using Domain.Errors;
using Domain.Shared;
using Infrastructure.Persistance;
using Tests.WorkCommunity.Application.UnitTests.Mocks;

namespace Tests.WorkCommunity.Application.UnitTests.ParkingSlots.Queries
{
	public class GetFilteredParkingSlotsHandlerTests
	{
		public DbContextMock _mock;

		public GetFilteredParkingSlotsHandlerTests() {
			_mock = new("GetFilteredParkingSlots");
			_mock.seedParkingSlots();
		}

		[Fact]
		public async Task Handle_Should_ReturnSuccess_WhenFilteredByUserId() {
			//Setup
			using var context = new CommunityDbContext(_mock.contextOptions);
			int userId = _mock.users.First().Id;
			var query = new GetFilteredParkingSlotsQuery() { userId = userId};
			var handler = new GetFilteredParkingSlotsHandler(context);

			//Execute
			Result<List<ParkingSlot>> result = await handler.Handle(query, default);

			//Assert
			Assert.False(result.IsError);
			result.Value.ForEach(u => Assert.True(u.UserId == userId));
			context.Database.EnsureDeleted();
		}

		[Fact]
		public async Task Handle_Should_ReturnSuccess_WhenFilteredByOccupied() {
			//Setup
			using var context = new CommunityDbContext(_mock.contextOptions);
			int userId = _mock.users.First().Id;
			var query = new GetFilteredParkingSlotsQuery() { userId = userId };
			var handler = new GetFilteredParkingSlotsHandler(context);

			//Execute
			Result<List<ParkingSlot>> result = await handler.Handle(query, default);

			//Assert
			Assert.False(result.IsError);
			result.Value.ForEach(u => Assert.True(u.UserId != null));
			context.Database.EnsureDeleted();
		}

		[Fact]
		public async Task Handle_Should_ReturnSuccess_WhenEmptyFilter() {
			//Setup
			using var context = new CommunityDbContext(_mock.contextOptions);
			int userId = _mock.users.First().Id;
			var query = new GetFilteredParkingSlotsQuery();
			var handler = new GetFilteredParkingSlotsHandler(context);

			//Execute
			Result<List<ParkingSlot>> result = await handler.Handle(query, default);

			//Assert
			Assert.True(result.IsError);
			Assert.True(result.Error.GetType() == typeof(InternalError));
			context.Database.EnsureDeleted();
		}
	}
}
