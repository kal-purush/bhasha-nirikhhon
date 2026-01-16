using System;
using System.Collections;
namespace IllySystems
{
  public class ObjectB : IObjectClass
  {
    Hashtable objectBHashTable = new Hashtable();
    public string surnameB { get; set; }
    public int ageB { get; set; }
    public DateTime dateOfBirthB { get; set; }
    public EmploymentHistory objectBEmploymentHistory = new EmploymentHistory("Marketing", new DateTime(2014, 06, 19), 37.5);

    public ObjectB() { }

     public ObjectB(string surname, int age, DateTime dateOfBirth)
     {
       surnameB = surname;
       ageB = age;
       dateOfBirthB = dateOfBirth;
     }       

    public  Hashtable GetHashtable()
    {
      objectBHashTable.Clear();
      objectBHashTable.Add("Surname", surnameB);
      objectBHashTable.Add("Age", ageB);
      objectBHashTable.Add("Date of Birth", dateOfBirthB.ToString("MM dd yyyy"));
      objectBHashTable.Add("Employment History",  "Dept: " + objectBEmploymentHistory.Department
        + " " + "started:  " + objectBEmploymentHistory.startDate.ToString("MM dd yyyy")
        + " hour worked  " + " " + objectBEmploymentHistory.HourWorked);
      return objectBHashTable;
    }
  }
}