using AutoMapper;
using Moq;
using NUnit.Framework;
using ShareCar.Db.Repositories.RideRequest_Repository;
using ShareCar.Dto;
using ShareCar.Logic.Address_Logic;
using ShareCar.Logic.Passenger_Logic;
using ShareCar.Logic.Ride_Logic;
using ShareCar.Logic.RideRequest_Logic;
using ShareCar.Logic.User_Logic;
using System;
using System.Collections.Generic;
using System.Text;

namespace ShareCar.Test
{
    public class RideRequestTest
    {
        Mock<IRideRequestRepository> rideRequestRepository = new Mock<IRideRequestRepository>();
        Mock<IRideLogic> rideLogic = new Mock<IRideLogic>();
        Mock<IPassengerLogic> passengerLogic = new Mock<IPassengerLogic>();
        Mock<IAddressLogic> addressLogic = new Mock<IAddressLogic>();
        Mock<IMapper> mapper = new Mock<IMapper>();
        Mock<IUserLogic> userLogic = new Mock<IUserLogic>();

        [Test]
        public void SortRequests_UnsortedRequests_SortedRequests()
        {
            List<RideRequestDto> requests = new List<RideRequestDto>
            {
                new RideRequestDto
                {
                    RequestId = 1,
                    SeenByPassenger = true,
                    Status = Status.ACCEPTED,
                },
                new RideRequestDto
                {
                    RequestId = 2,
                    SeenByPassenger = true,
                    Status = Status.WAITING,
                },
                new RideRequestDto
                {
                    RequestId = 3,
                    SeenByPassenger = false,
                    Status = Status.ACCEPTED,
                },

            };

            var rideRequestLogic = new RideRequestLogic(rideRequestRepository.Object, addressLogic.Object, userLogic.Object, mapper.Object, passengerLogic.Object, rideLogic.Object);

           var result = rideRequestLogic.SortRequests(requests);

            Assert.AreEqual(result[0].RequestId, 3);
            Assert.AreEqual(result[1].RequestId, 2);
            Assert.AreEqual(result[2].RequestId, 1);

        }
    }
}