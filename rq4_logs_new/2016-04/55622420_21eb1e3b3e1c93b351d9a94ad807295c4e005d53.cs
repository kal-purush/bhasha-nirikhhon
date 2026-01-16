using System;

class Animal
{
  /*
    Private class variable section
   */
  private string _name = "";
  private string _food = "";

  /*
    Public properties section
   */
  public string species { get; set; }

  public string name {
    get
    {
      return _name;
    }
  }

  public string food {
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

  /*
    Public methods section
   */
  public void eat(string food) {
    _food = food;
    Console.Write("Currently eating {0}", food);
  }

}

/*
  Firefox class inherits from Animal class
 */
class Firefox : Animal {

  /*
    Public properties section. Any Firefox object
    will have these properties AND any properties
    defined in Animal
   */
  public double furThickness { get; set; }
  public int cutenessLevel { get; set; }
}