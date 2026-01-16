using System;
using System.Collections;

namespace IllySystems
{
  public class ObjectA : IObjectClass
  {
    Hashtable objectAHashTable = new Hashtable();
    public string nameA { get; set; }
    public int ageA { get; set; }
    public DateTime dateOfBirthA { get; set; }
    public EmploymentHistory objectAEmploymentHistory = new EmploymentHistory("Marketing", new DateTime(2014, 06, 19), 37.5);

    public ObjectA() { }

    public ObjectA(string surname, int age, DateTime dateOfBirth)
    {
      nameA = surname;
      ageA = age;
      dateOfBirthA = dateOfBirth;
    }

    public Hashtable GetHashtable()
    {
      objectAHashTable.Clear();
      objectAHashTable.Add("Surname", nameA);
      objectAHashTable.Add("Age", ageA);
      objectAHashTable.Add("Date of Birth", dateOfBirthA.ToString("MM dd yyyy"));
      objectAHashTable.Add("Employment History", "Dept: " + objectAEmploymentHistory.Department
        + " " + "started:  " + objectAEmploymentHistory.startDate.ToString("MM dd yyyy")
        + " hour worked  " + " " + objectAEmploymentHistory.HourWorked);
      return objectAHashTable;
    }
  }
}