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
    public class RentalControllerTests
    {     
            RentalController sut;
            IRepository<Rental> rentalRepo;

            public RentalControllerTests()
            {
                rentalRepo = Substitute.For<IRepository<Rental>>();
                sut = new RentalController(rentalRepo);
            }

        [Fact]
        
        public void RentalDate_IsApproved_On_RentalModel()
        {
            bool isApproved = sut.IsApproved;
            Assert.False(isApproved);
        }

        [Fact]
        public void Get_Rental_Returns_A_Rental()
        {
            var expectedRental = new Rental();
            rentalRepo.GetById(1).Returns(expectedRental);

            var result = sut.GetRental(1);

            Assert.Equal(expectedRental, result);
        }

    }
}