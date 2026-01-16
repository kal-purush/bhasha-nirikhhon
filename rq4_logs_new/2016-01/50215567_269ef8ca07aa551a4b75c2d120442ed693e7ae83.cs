using System;
namespace IllySystems
{
  class DemoCompareObjects
  {
    public static void Main()
    {
      ObjectA person_A = new ObjectA("A", 26, new DateTime(1990, 6, 10));
      ObjectB person_B = new ObjectB("B", 10, new DateTime(2006, 7, 10));

      person_B.objectBEmploymentHistory.Department = "IT";
      person_B.objectBEmploymentHistory.startDate = new DateTime(2012, 02, 09);
      person_B.objectBEmploymentHistory.HourWorked = 38.5;

      person_A.objectAEmploymentHistory.Department = "HR";
      person_A.objectAEmploymentHistory.startDate = new DateTime( 2010, 05, 09);
      person_A.objectAEmploymentHistory.HourWorked = 37.5;

      new Auditor().CompareObjects(person_A, person_B);      
    }
  }
}