using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZoolandiaRazor.Models;
using System.Collections.Generic;
using System.Linq;
using Moq;
using System.Data.Entity;

namespace ZoolandiaRazor.Tests.DAL
{
    [TestClass]
    public class ZooRepositoryTests
    {
        Mock<ZooContext> mock_context { get; set; }

        Mock<DbSet<Animal>> mock_animal_table { get; set; }
        List<Animal> animal_list { get; set; }

        Mock<DbSet<Employee>> mock_employee_table { get; set; }
        List<Animal> employee_list { get; set; }

        Mock<DbSet<Habitat>> mock_habitat_table { get; set; }
        List<Animal> habitat_list { get; set; }

        Mock<DbSet<Species>> mock_species_table { get; set; }
        List<Animal> species_list { get; set; }

        ZooRepositoryTests repo { get; set; }

        [TestMethod]
        public void ConnectMocksToDatastore()
        {
            var animal_queryable_list = animal_list.AsQueryable();
            var employee_queryable_list = employee_list.AsQueryable();
            var habitat_queryable_list = habitat_list.AsQueryable();
            var species_queryable_list = species_list.AsQueryable();

            //Animal class
            mock_animal_table.As<IQueryable<Animal>>().Setup(m => m.Provider).Returns(animal_queryable_list.Provider);
            mock_animal_table.As<IQueryable<Animal>>().Setup(m => m.Expression).Returns(animal_queryable_list.Expression);
            mock_animal_table.As<IQueryable<Animal>>().Setup(m => m.ElementType).Returns(animal_queryable_list.ElementType);
            mock_animal_table.As<IQueryable<Animal>>().Setup(m => m.GetEnumerator()).Returns(() => animal_queryable_list.GetEnumerator());

            //Employee Class
            mock_employee_table.As<IQueryable<Employee>>().Setup(m => m.Provider).Returns(employee_queryable_list.Provider);
            mock_employee_table.As<IQueryable<Employee>>().Setup(m => m.Expression).Returns(employee_queryable_list.Expression);
            mock_employee_table.As<IQueryable<Employee>>().Setup(m => m.ElementType).Returns(employee_queryable_list.ElementType);
            mock_employee_table.As<IQueryable<Employee>>().Setup(m => m.GetEnumerator()).Returns(() => employee_queryable_list.GetEnumerator());

            //Habitat Class
            mock_habitat_table.As<IQueryable<Habitat>>().Setup(m => m.Provider).Returns(habitat_queryable_list.Provider);
            mock_habitat_table.As<IQueryable<Habitat>>().Setup(m => m.Expression).Returns(habitat_queryable_list.Expression);
            mock_habitat_table.As<IQueryable<Habitat>>().Setup(m => m.ElementType).Returns(habitat_queryable_list.ElementType);
            mock_habitat_table.As<IQueryable<Habitat>>().Setup(m => m.GetEnumerator()).Returns(() => habitat_queryable_list.GetEnumerator());

            //Species Class
            mock_species_table.As<IQueryable<Species>>().Setup(m => m.Provider).Returns(species_queryable_list.Provider);
            mock_species_table.As<IQueryable<Species>>().Setup(m => m.Expression).Returns(species_queryable_list.Expression);
            mock_species_table.As<IQueryable<Species>>().Setup(m => m.ElementType).Returns(species_queryable_list.ElementType);
            mock_species_table.As<IQueryable<Species>>().Setup(m => m.GetEnumerator()).Returns(() => species_queryable_list.GetEnumerator());

            mock_context.Setup(c => c.Animal).Returns(mock_animal_table.Object);
            mock_context.Setup(c => c.Employee).Returns(mock_employee_table.Object);
            mock_context.Setup(c => c.Habitat).Returns(mock_habitat_table.Object);
            mock_context.Setup(c => c.Species).Returns(mock_species_table.Object);


            mock_animal_table.Setup(t => t.Add(It.IsAny<Animal>())).Callback((Animal a) => animal_list.Add(a));
            mock_employee_table.Setup(t => t.Add(It.IsAny<Employee>())).Callback((Employee a) => employee_list.Add(a));
            mock_habitat_table.Setup(t => t.Add(It.IsAny<Habitat>())).Callback((Habitat a) => habitat_list.Add(a));
            mock_species_table.Setup(t => t.Add(It.IsAny<Species>())).Callback((Species a) => species_list.Add(a));


        }


        [TestInitialize]
        public void Initialize()
        {
            mock_context = new Mock<Animal>();
            mock_animal_table = new Mock<DbSet<Animal>>();
            animal_list = new List<Animal>(); // Fake
            repo = new ZooRepository(mock_context.Object);

            ConnectMocksToDatastore();
        }
    }
}