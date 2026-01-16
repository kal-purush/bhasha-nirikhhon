using System;
using System.Collections.Generic;
using HairSalon.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySql.Data.MySqlClient;
using static HairSalon.Startup;


namespace HairSalon.Tests
  {
  [TestClass]
  public class SpecialtyTests : IDisposable
  {

    public SpecialtyTests ()
    {
      DBConfiguration.ConnectionString = "server=localhost;user id=root;password=root;port=8889;database=brian_nelson_test;";
    }

    public void Dispose()
    {
        Specialty.ClearAll();
        Stylist.ClearAll();
        Client.ClearAll();
    }

    [TestMethod] // test to see if database is cleared between tests
    public void GetAll_SpecialtysEmptyAtFirst_0()
    {
        Specialty.ClearAll();
        Stylist.ClearAll();
        Client.ClearAll();
        int result = Specialty.GetAll().Count;
        Assert.AreEqual(0, result);
    }

    [TestMethod] // test to see if .Create and is creating in DB
    public void Create_SpecialtyAddedCorrectly_True()
    {
        Specialty.ClearAll();
        Stylist.ClearAll();
        Client.ClearAll();
        Specialty newSpecialty = new Specialty ("Mens");
        newSpecialty.Create();

        Specialty test = Specialty.GetAll()[0];
        Assert.AreEqual("Mens", test.Description);
    }

    [TestMethod] // test to see if getall returns all lines in DB
    public void GetAll_GetAllSpecialtys_Int ()
    {
        Specialty.ClearAll();
        Stylist.ClearAll();
        Client.ClearAll();
        Specialty newSpecialty1 = new Specialty ("Mens");
        newSpecialty1.Create();
        Specialty newSpecialty2 = new Specialty ("Kids");
        newSpecialty2.Create();
        Specialty newSpecialty3 = new Specialty ("Womens");
        newSpecialty3.Create();
        int result = Specialty.GetAll().Count;
        Assert.AreEqual(result, 3);
    }

    [TestMethod] // test to see if delete removes the proper db item
    public void Delete_DeleteSpecialtys_Count ()
    {
        Specialty.ClearAll();
        Stylist.ClearAll();
        Client.ClearAll();
        Specialty newSpecialty1 = new Specialty ("Mens");
        newSpecialty1.Create();
        Specialty newSpecialty2 = new Specialty ("Kids");
        newSpecialty2.Create();
        Specialty newSpecialty3 = new Specialty ("Womens");
        newSpecialty3.Create();
        Specialty.Delete(newSpecialty2.Id);
        int result = Specialty.GetAll().Count;
        Assert.AreEqual(result, 2);
    }

    [TestMethod] // test to see if find function returns proper stylist
    public void Find_FindSpecialty_Name ()
    {
        Specialty.ClearAll();
        Stylist.ClearAll();
        Client.ClearAll();
        Specialty newSpecialty1 = new Specialty ("Mens");
        newSpecialty1.Create();
        Specialty test = Specialty.Find(newSpecialty1.Id);
        Assert.AreEqual("Mens", test.Description);
    }

    [TestMethod]
    public void Update_ChangeSpecialtyNameCorrectly_True()
    {
        Specialty.ClearAll();
        Stylist.ClearAll();
        Client.ClearAll();

        Specialty newSpecialty1 = new Specialty ("Mnnes");
        newSpecialty1.Create();
        newSpecialty1.Update("Mens");
        Specialty testSpecialty = Specialty.Find(newSpecialty1.Id);

        Assert.AreEqual("Mens", testSpecialty.Description);
    }
  }
}