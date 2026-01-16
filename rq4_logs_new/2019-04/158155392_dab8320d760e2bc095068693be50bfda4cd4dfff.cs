using System;
using DataAccessLayer;
using BroadwayBuilder.Api.Controllers;
using BroadwayBuilder.Api.Models;
using System.Web.Http.Results;
using System.Net;
using ServiceLayer.Services;
using System.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace BroadwayBuilder.Api.Tests
{
    [TestClass]
    public class ProductionControllerTest
    {
        [TestMethod]
        public void ProductionController_UploadProgram_Pass()
        {
            // Arrange
            var dbcontext = new BroadwayBuilderContext();
            var productionService = new ProductionService(dbcontext);



            // Act

            // Assert
        }

        //[TestMethod]
        //public void ProductionController_CreateProduction_Pass()
        //{
        //    // Arrange
        //    var dbcontext = new BroadwayBuilderContext();
        //    var theaterService = new TheaterService(dbcontext);

        //    var theater = new Theater()
        //    {
        //        TheaterName = "Some Theater",
        //        StreetAddress = "Theater St",
        //        State = "CA",
        //        City = "LA",
        //        CompanyName = "Regal",
        //        Country = "US",
        //        PhoneNumber = "123456789"
        //    };

        //    theaterService.CreateTheater(theater);
        //    dbcontext.SaveChanges();

        //    var productionService = new ProductionService(dbcontext);

        //    var productionController = new ProductionController();


        //    // Act
        //    var actionResult = productionController.CreateProduction(new Production()
        //    {
        //        ProductionName = "The New Production",
        //        DirectorFirstName = "New",
        //        DirectorLastName = "New",
        //        Street = "Anahiem",
        //        City = "New Town",
        //        StateProvince = "California",
        //        Country = "United States",
        //        Zipcode = "919244",
        //        TheaterID = theater.TheaterID
        //    });

        //    var response = actionResult as CreatedAtRouteNegotiatedContentResult<Production>;


        //    // Assert
        //    theaterService.DeleteTheater(theater);
        //    dbcontext.SaveChanges();
        //    productionService.DeleteProduction(response.Content.ProductionID);
        //    dbcontext.SaveChanges();
        //    Assert.IsNotNull(response);
        //    Assert.AreEqual("production/Create", response.RouteName);

        //}

        [TestMethod]
        public void ProductionController_GetProductionById_Pass()
        {
            // Arrange
            var dbcontext = new BroadwayBuilderContext();
            var theaterService = new TheaterService(dbcontext);

            var theater = new Theater()
            {
                TheaterName = "Some Theater",
                StreetAddress = "Theater St",
                State = "CA",
                City = "LA",
                CompanyName = "Regal",
                Country = "US",
                PhoneNumber = "123456789"
            };

            theaterService.CreateTheater(theater);
            dbcontext.SaveChanges();

            var productionService = new ProductionService(dbcontext);

            var production = new Production
            {
                ProductionName = "The Pajama Game",
                DirectorFirstName = "Doris",
                DirectorLastName = "Day",
                City = "San Diego",
                StateProvince = "California",
                Country = "U.S",
                TheaterID = theater.TheaterID,
                Street = "123 Sesame St",
                Zipcode = "91911"
            };

            productionService.CreateProduction(production);
            dbcontext.SaveChanges();

            var productionController = new ProductionController();

            // Act
           var actionResult = productionController.GetProductionById(production.ProductionID);

            var response = actionResult as OkNegotiatedContentResult<Production>;


            productionService.DeleteProduction(response.Content.ProductionID);
            dbcontext.SaveChanges();
            theaterService.DeleteTheater(theater);
            dbcontext.SaveChanges();
           
            // Assert
            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Content);
            Assert.AreEqual(production.ProductionID, response.Content.ProductionID);

        }

        [TestMethod]
        public void ProductionController_GetProductions_Pass()
        {
            // Arrange
            var dbcontext = new BroadwayBuilderContext();
            var theaterService = new TheaterService(dbcontext);

            var theater = new Theater()
            {
                TheaterName = "Some Theater",
                StreetAddress = "Theater St",
                State = "CA",
                City = "LA",
                CompanyName = "Regal",
                Country = "US",
                PhoneNumber = "123456789"
            };

            theaterService.CreateTheater(theater);
            dbcontext.SaveChanges();


            var productionService = new ProductionService(dbcontext);

            var production1 = new Production()
            {
                ProductionName = "The Lion King 14",
                DirectorFirstName = "Joan",
                DirectorLastName = "Doe",
                Street = "123 Anahiem St",
                City = "Long Beach",
                StateProvince = "California",
                Country = "United States",
                Zipcode = "919293",
                TheaterID = theater.TheaterID
            };


            productionService.CreateProduction(production1);
            dbcontext.SaveChanges();

            var production2 = new Production()
            {
                ProductionName = "The Lion King 15",
                DirectorFirstName = "Joan",
                DirectorLastName = "Doe",
                Street = "123 Anahiem St",
                City = "Long Beach",
                StateProvince = "California",
                Country = "United States",
                Zipcode = "919293",
                TheaterID = theater.TheaterID
            };

            productionService.CreateProduction(production2);
            dbcontext.SaveChanges();

            var productionDateTime1 = new ProductionDateTime()
            {
                Date = DateTime.Parse("3/23/2019 3:22:29 PM"),
                Time = TimeSpan.Parse("10:30:00"),
                ProductionID = production1.ProductionID
            };

            productionService.CreateProductionDateTime(productionDateTime1);
            dbcontext.SaveChanges();

            var productionDateTime2 = new ProductionDateTime()
            {
                Date = DateTime.Parse("3/29/2019 3:22:29 PM"),
                Time = TimeSpan.Parse("5:30:00"),
                ProductionID = production2.ProductionID
            };

            productionService.CreateProductionDateTime(productionDateTime2);
            dbcontext.SaveChanges();


            var productionController = new ProductionController();

            // Act
            var actionResult = productionController.GetProductions(currentDate: new DateTime(2019, 3, 1));

            var response = actionResult as OkNegotiatedContentResult<List<ProductionResponseModel>>;

            var productions = response.Content;

            var expected = true;
            var actual = false;

            if (productions.Count > 0)
            {
                actual = true;
            }

            productionService.DeleteProductionDateTime(productionDateTime2);
            dbcontext.SaveChanges();
            productionService.DeleteProductionDateTime(productionDateTime1);
            dbcontext.SaveChanges();
            productionService.DeleteProduction(production1.ProductionID);
            dbcontext.SaveChanges();
            productionService.DeleteProduction(production2.ProductionID);
            dbcontext.SaveChanges();
            theaterService.DeleteTheater(theater);
            dbcontext.SaveChanges();

            // Assert
            Assert.IsNotNull(response.Content);
            Assert.AreEqual(expected, actual);

        }

        [TestMethod]
        public void ProductionController_UpdateProduction_Pass()
        {
            // Arrange
            var dbcontext = new BroadwayBuilderContext();
            var theaterService = new TheaterService(dbcontext);

            var theater = new Theater()
            {
                TheaterName = "The Magicians",
                StreetAddress = "Pantene",
                State = "CA",
                City = "LA",
                CompanyName = "123 Sesame St",
                Country = "US",
                PhoneNumber = "123456789"
            };

            theaterService.CreateTheater(theater);
            dbcontext.SaveChanges();


            var productionService = new ProductionService(dbcontext);

            var production = new Production()
            {
                ProductionName = "The Lion King",
                DirectorFirstName = "Jane",
                DirectorLastName = "Doe",
                Street = "Anahiem",
                City = "Long Beach",
                StateProvince = "California",
                Country = "United States",
                Zipcode = "919293",
                TheaterID = theater.TheaterID
            };

            production.ProductionName = "The Updated Lion King";
            production.StateProvince = "Utah";
            production.DirectorLastName = "Mangos";

            productionService.CreateProduction(production);
            dbcontext.SaveChanges();

            var productionController = new ProductionController();

            // Act

            var actionResult = productionController.UpdateProduction(production);

            var response = actionResult as OkNegotiatedContentResult<Production>;
            var updatedProduction = response.Content;

            productionService.DeleteProduction(response.Content.ProductionID);
            dbcontext.SaveChanges();
            theaterService.DeleteTheater(theater);
            dbcontext.SaveChanges();
            // Assert
            Assert.IsNotNull(response);
            Assert.AreEqual(production.ProductionID, updatedProduction.ProductionID);
            Assert.AreEqual("The Updated Lion King", updatedProduction.ProductionName);
            Assert.AreEqual("Utah", updatedProduction.StateProvince);
            Assert.AreEqual("Mangos", updatedProduction.DirectorLastName);
            
        }

        [TestMethod]
        public void ProductionController_DeleteProduction_Pass()
        {
            // Arrange
            var dbcontext = new BroadwayBuilderContext();
            var theaterService = new TheaterService(dbcontext);

            var theater = new Theater()
            {
                TheaterName = "Some Theater",
                StreetAddress = "Theater St",
                State = "CA",
                City = "LA",
                CompanyName = "Regal",
                Country = "US",
                PhoneNumber = "123456789"
            };

            theaterService.CreateTheater(theater);
            dbcontext.SaveChanges();

            var productionService = new ProductionService(dbcontext);

            var production = new Production
            {
                ProductionName = "The Pajama Game",
                DirectorFirstName = "Doris",
                DirectorLastName = "Day",
                City = "San Diego",
                StateProvince = "California",
                Country = "U.S",
                TheaterID = theater.TheaterID,
                Street = "123 Sesame St",
                Zipcode = "91911"
            };

            productionService.CreateProduction(production);
            dbcontext.SaveChanges();

            var productionController = new ProductionController();

            // Act
            var actionResult = productionController.deleteProduction(production.ProductionID);

            var response = actionResult as OkNegotiatedContentResult<string>;

            var dbcontext_ = new BroadwayBuilderContext();
            var theaterService_ = new TheaterService(dbcontext);
            var theater_ = theaterService.GetTheaterByID(theater.TheaterID);
            theaterService_.DeleteTheater(theater_);
            dbcontext_.SaveChanges();
           
            // Assert
            Assert.IsNotNull(response);
            Assert.AreEqual("Production deleted succesfully", response.Content);

        }
    }




}