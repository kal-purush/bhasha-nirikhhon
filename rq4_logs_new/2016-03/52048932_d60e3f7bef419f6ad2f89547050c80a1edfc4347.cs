using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Generics
{
  class Program
  {
    static void Main(string[] args)
    {

      //GenericTest();

      GenshowValue<int>(10);                   //Works fine
      GenshowValue<string>("This is string");  //Compile error because string is not value type
      GenshowValue<double>(10.7545);
    }

    /// <summary>
    /// Generic methods
    /// </summary>
    /// <typeparam name="X"></typeparam>
    /// <param name="val"></param>
    static void GenshowValue<X>(X val)
    {
      Console.WriteLine(val.ToString());
    }

    /// <summary>
    /// Generic classes
    /// </summary>
    static void GenericTest()
    {
      // Use the generic type Test with an int type parameter.
      Test<int> test1 = new Test<int>(5);
      // Call the Write method.
      test1.Write();

      // Use the generic type Test with a string type parameter.
      Test<string> test2 = new Test<string>("cat");
      test2.Write();
    }
  }

  class Test<T>
  {
    T _value;

    public Test(T t)
    {
      // The field has the same type as the parameter.
      this._value = t;
    }

    public void Write()
    {
      Console.WriteLine(this._value);
    }
  }
}