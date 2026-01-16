using System.Collections;
using System;
namespace IllySystems
{
  public abstract class ObjectClass
  {
    public string Surname { get; set; }
    public int SecurityNumber { get; set; }
    public DateTime DateOfBirth { get; set; }
    public abstract Hashtable GetHashtable();
  }
}