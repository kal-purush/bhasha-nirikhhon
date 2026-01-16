using AutoMapper;
using Moq;
using NUnit.Framework;
using ShareCar.Db.Entities;
using ShareCar.Db.Repositories.User_Repository;
using ShareCar.Dto.Identity;
using ShareCar.Logic.Passenger_Logic;
using ShareCar.Logic.User_Logic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ShareCar.Test
{
    public class UserLogicTest
    {
        Mock<IUserRepository> userRepository = new Mock<IUserRepository>();
        Mock<IPassengerLogic> passengerLogic = new Mock<IPassengerLogic>();
        Mock<IMapper> mapper = new Mock<IMapper>();

        [Test]
        public void GetWinnerBoard_10Users_Returns5HighestScoores()
        {
            var userLogic = new UserLogic(userRepository.Object, passengerLogic.Object, mapper.Object);

            List<User> allUsers = new List<User> {
                new User{Email = "1"},
                new User{Email = "2"},
                new User{Email = "3"},
                new User{Email = "4"},
                new User{Email = "5"},
                new User{Email = "6"},
                new User{Email = "7"},
                new User{Email = "8"},
                new User{Email = "9"},
                new User{Email = "10"},
            };

            passengerLogic.Setup(x => x.GetUsersPoints("1")).Returns(6);
            passengerLogic.Setup(x => x.GetUsersPoints("2")).Returns(2);
            passengerLogic.Setup(x => x.GetUsersPoints("3")).Returns(5);
            passengerLogic.Setup(x => x.GetUsersPoints("4")).Returns(6);
            passengerLogic.Setup(x => x.GetUsersPoints("5")).Returns(67);
            passengerLogic.Setup(x => x.GetUsersPoints("6")).Returns(8);
            passengerLogic.Setup(x => x.GetUsersPoints("7")).Returns(0);
            passengerLogic.Setup(x => x.GetUsersPoints("8")).Returns(9);
            passengerLogic.Setup(x => x.GetUsersPoints("9")).Returns(11);
            passengerLogic.Setup(x => x.GetUsersPoints("10")).Returns(21);

            mapper.Setup(x => x.Map<User, UserDto>(allUsers[0])).Returns(new UserDto { Email = allUsers[0].Email });
            mapper.Setup(x => x.Map<User, UserDto>(allUsers[1])).Returns(new UserDto { Email = allUsers[1].Email });
            mapper.Setup(x => x.Map<User, UserDto>(allUsers[2])).Returns(new UserDto { Email = allUsers[2].Email });
            mapper.Setup(x => x.Map<User, UserDto>(allUsers[3])).Returns(new UserDto { Email = allUsers[3].Email });
            mapper.Setup(x => x.Map<User, UserDto>(allUsers[4])).Returns(new UserDto { Email = allUsers[4].Email });
            mapper.Setup(x => x.Map<User, UserDto>(allUsers[5])).Returns(new UserDto { Email = allUsers[5].Email });
            mapper.Setup(x => x.Map<User, UserDto>(allUsers[6])).Returns(new UserDto { Email = allUsers[6].Email });
            mapper.Setup(x => x.Map<User, UserDto>(allUsers[7])).Returns(new UserDto { Email = allUsers[7].Email });
            mapper.Setup(x => x.Map<User, UserDto>(allUsers[8])).Returns(new UserDto { Email = allUsers[8].Email });
            mapper.Setup(x => x.Map<User, UserDto>(allUsers[9])).Returns(new UserDto { Email = allUsers[9].Email });


            userRepository.Setup(x => x.GetAllUsers()).Returns(allUsers);

            var points = userLogic.GetWinnerBoard();
            var expectedKeys = new List<string> { "10", "9", };

            var actualKeys = new List<UserDto>(points.Keys);
            var keyStrings = new List<string>(actualKeys.Select(x => x.Email));

            Assert.AreEqual(points.ElementAt(0).Value, 67);
            Assert.AreEqual(points.ElementAt(1).Value, 21);
            Assert.AreEqual(points.ElementAt(2).Value, 11);
            Assert.AreEqual(points.ElementAt(3).Value, 9);
            Assert.AreEqual(points.ElementAt(4).Value, 8);

            Assert.AreEqual(points.Count, 5);

            Assert.IsTrue(expectedKeys.TrueForAll(x => keyStrings.Contains(x)));

        }

    }
}