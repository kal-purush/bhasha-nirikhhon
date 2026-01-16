using System;
using System.Collections.Generic;
using System.Linq;
using freshop.Models;
using freshop.Repositories;
using freshop.Services;
using NUnit.Framework;
using Dapper;
using SQLitePCL;
using System.Data.SQLite;
using System.Transactions;

namespace freshop.IntegrationTests.Services
{
    public class ProductsServiceTests
    {
        private ProductsService productsService;

        [SetUp]
        public void SetUp()
        {
            this.productsService = new ProductsService(new ProductsRepository("Data Source=/Users/fredrik/Projects/freshop/freshop/database.db;Version=3;"));
        }

        [Test]
        public void Get_ReturnsResultsFromDatabase()
        {
            // Act
            var results = this.productsService.Get();

            // Assert
            Assert.That(results.Count, Is.EqualTo(16));
            Assert.That(results[15].pn, Is.EqualTo("SFP-10G-SR"));
            Assert.That(results[15].qty, Is.EqualTo(20));
            Assert.That(results[15].id, Is.EqualTo(16));
        }

        [Test]
        public void Get_PN_ReturnsResultsFromDatabase()
        {
            //Arrange
            const string key = "WS-C3560X-24P-L";

            // Act
            var result = this.productsService.Get(key);

            // Assert
            Assert.That(result[0].id, Is.EqualTo(2));
            Assert.That(result[0].price, Is.EqualTo(3389));
            Assert.That(result[0].qty, Is.EqualTo(15));
            Assert.That(result[0].model, Is.EqualTo("Cisco Catalyst 3560X-24P-L"));
        }
    }
}