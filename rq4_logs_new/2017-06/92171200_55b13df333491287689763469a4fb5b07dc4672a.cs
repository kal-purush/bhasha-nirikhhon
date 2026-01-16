using Microsoft.VisualStudio.TestTools.UnitTesting;
using eDnevnikDev.Controllers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using eDnevnikDev.Models;
using Moq;
using System.Data.Entity;
using System.Web.Mvc;

namespace eDnevnikDev.Controllers.Tests
{
    [TestClass]
    public class UceniciControllerTests
    {
        [TestMethod]
        public void IndexTest()
        {
            var data = new List<Ucenik>
            {
                new Ucenik {UcenikID = 1, Ime = "Petar", Prezime = "Petrovic", Adresa = "Kneza Milosa 12", BrojTelefonaRoditelja = "+381-11/1234567", ImeMajke = "Jovana", ImeOca = "Jovana", JMBG = "1234567891234", MestoPrebivalista = "Blace", MestoRodjenjaId = 1, OdeljenjeId = 1, PrezimeMajke = "Petrovic", PrezimeOca = "Petrovic", Razred = 1, SmerID = 2, Vanredan = false }
            }.AsQueryable();

            var mockSet = new Mock<DbSet<Ucenik>>();
            mockSet.As<IQueryable<Ucenik>>().Setup(m => m.Provider).Returns(data.Provider);
            mockSet.As<IQueryable<Ucenik>>().Setup(m => m.Expression).Returns(data.Expression);
            mockSet.As<IQueryable<Ucenik>>().Setup(m => m.ElementType).Returns(data.ElementType);
            mockSet.As<IQueryable<Ucenik>>().Setup(m => m.GetEnumerator()).Returns(data.GetEnumerator());

            var mockContext = new Mock<ApplicationDbContext>();
            mockContext.Setup(c => c.Ucenici).Returns(mockSet.Object);

            //var service = new BlogService(mockContext.Object);

            var kontroler = new UceniciController(mockContext.Object);

            var result = kontroler.Index() as ViewResult;
            var model = result.Model as IEnumerable<Ucenik>;
            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1, model.Count());
            Assert.AreEqual("Index", result.ViewName);
            
        }
    }
}