using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using IllySystems;
using System.Collections;
namespace UnitTestProjectCompareObject
{
  [TestClass]
  public class UnitTestCompareObjects
  {
    [TestMethod]
    public void ObjectsName_AreDifferent_ReturnOneChange()
    {
      //Arrange
      ObjectA objectA = new ObjectA("A", 10, new DateTime(2006, 7, 10));
      ObjectB objectB = new ObjectB("B", 10, new DateTime(2006, 7, 10));

      Auditor auditor = new Auditor();

      String result = "";

      //Act
      Hashtable ht = auditor.CompareObjects(objectA, objectB);
      foreach (DictionaryEntry de in ht)
      {
        result += de.Value;
      }

      //Assert
      Assert.AreEqual(result, "Change 1: " + "Object_A: A || Object_B: B ");

    }

    [TestMethod]
    public void ObjectsDateOfBirth_AreDifferent_ReturnOneChange()
    {
      //Arrange
      ObjectA objectA = new ObjectA("A", 10, new DateTime(2006, 8, 10));
      ObjectB objectB = new ObjectB("A", 10, new DateTime(2006, 7, 10));

      Auditor auditor = new Auditor();

      String result = "";

      //Act
      Hashtable ht = auditor.CompareObjects(objectA, objectB);
      foreach (DictionaryEntry de in ht)
      {
        result += de.Value;
      }

      //Assert
      Assert.AreEqual(result, "Change 1: Object_A: 08 10 2006 || Object_B: 07 10 2006 ");
    }

    [TestMethod]
    public void ObjectsDateOfBirthAndAge_AreDifferent_ReturnTwoChange()
    {
      // Arrange
      ObjectA objectA = new ObjectA("A", 10, new DateTime(2006, 8, 10));
      ObjectB objectB = new ObjectB("A", 26, new DateTime(2006, 7, 10));

      Auditor auditor = new Auditor();

      String result = "";

      // Act
      Hashtable ht = auditor.CompareObjects(objectA, objectB);
      foreach (DictionaryEntry de in ht)
      {
        result += de.Value;
      }

      // Assert
      Assert.AreEqual(result, "Change 1: Object_A: 08 10 2006 || Object_B: 07 10 2006 Change 2: Object_A: 10 || Object_B: 26 ");
    }
  }
}
