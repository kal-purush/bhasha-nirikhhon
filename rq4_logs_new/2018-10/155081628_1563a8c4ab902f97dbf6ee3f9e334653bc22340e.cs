using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PatientDemog.Data;
namespace PatientDemog.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            PatientRepository p = new PatientRepository();
            IList < Patient > list= p.GetAll();

            Patient pt = new Patient();
            pt.FirstName = "Shahenaz";
            pt.LastName = "Sangoli";
            pt.Gender = "Female";
            pt.BirthDate = new DateTime(1999, 5, 2);
            p.Insert(pt);

            Patient pt2 = new Patient();
            pt2.FirstName = "Kevin";
            pt2.LastName = "Emerson";
            pt2.Gender = "Male";
            pt2.BirthDate = new DateTime(1997, 4, 2);
            pt2.Phones.Add(new Phone { Type = "Home", PhoneNumber = "121212" });
            p.Insert(pt2);
            p.Dispose();

        }
    }
}