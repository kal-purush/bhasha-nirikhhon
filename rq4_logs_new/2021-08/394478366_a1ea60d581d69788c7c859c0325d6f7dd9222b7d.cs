using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TeamVoldemort.Menu
{
  class DisplayMenu
  {

    public int Display()
    {
      Console.WriteLine("QuikTrip Management Systems");

      Console.WriteLine("1. Enter District Sales");
      Console.WriteLine("2. Generate District Report");
      Console.WriteLine("3. Add New Employee");
      Console.WriteLine("4. Add a Store/District");
      Console.WriteLine("5. Exit");
      var result = Console.ReadLine();
      return Int32.Parse(result);
    }
  }
}