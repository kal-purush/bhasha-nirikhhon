using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zoolandia
{
  class Animal
  {
    /*
      Private scoped variable that are only accessible within
      the Animal class.
     */
    private string _name = "";
    private string _food = "";
    private Species _species;

    public Species species
    {
      get
      {
        return _species;
      }
      set
      {
        _species = value;
      }
    }


    /*
      Public property that is accessible by other classes but
      can only be read, not manipulated.
     */
    public string name
    {
      get
      {
        return _name;
      }
      set
      {
        _name = value;
      }
    }

    /*
      Public property that is accessible by other classes that
      can be read and set, but with some conditional logic that
      checks the value before the private variable is set.

      Animal adam = new Animal();
      adam.food = "Banana";   <-- will compile
      adam.food = "";         <-- will not compile
     */
    public string food
    {
      get
      {
        return _food;
      }
      set
      {
        if (value != "")
        {
          _food = value;
        }
      }
    }

    public Animal(string name)
    {
      _name = name;
    }

    /*
      Your first method on a class.
     */
    public virtual void eat(string food)
    {
      _food = food;
      Console.WriteLine("Currently eating {0}", food);
    }
  }

}