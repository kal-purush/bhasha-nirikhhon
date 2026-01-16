using System;
using System.Collections;
using System.IO;

namespace IllySystems
{
  public class Auditor
  {
    //can specify the path here
    public string PathToOutPutChange = string.Empty;
    //public void CompareObjects(Hashtable hta, Hashtable htb)

  

    public  Hashtable CompareObjects(ObjectA objecta, ObjectB objectb)
    { 
      Hashtable changeHtable = new Hashtable();
      int counter = 0;
      foreach (DictionaryEntry entryA in objecta.GetHashtable())
      {

        foreach (DictionaryEntry entryB in objectb.GetHashtable())
        {
          if ((entryA.Key.Equals(entryB.Key)) && (!entryA.Value.Equals(entryB.Value)))
          {
            counter++;
            changeHtable.Add(entryA.Key, "Change "+ counter +": "+ "Object_A: " + entryA.Value + " || Object_B: " + entryB.Value + " ");
          }
        }
      }

      WriteDifference(changeHtable);
      WriteDifferenceToStream(changeHtable);

      return changeHtable;
    }

    private  void WriteDifference(Hashtable changeHt)
    {
      if (changeHt.Count != 0)
      {
        Console.WriteLine("Change are:");
        foreach (DictionaryEntry entrywithchanges in changeHt)
        {
          Console.WriteLine(entrywithchanges.Key);
          Console.WriteLine(entrywithchanges.Value);
        }
      }
      else
      {
        Console.WriteLine("There is no change.");
      }
    }

    private void WriteDifferenceToStream(Hashtable changeHt)
    {
      if (changeHt.Count != 0)
      {
        using (StreamWriter changefile = new StreamWriter(@"change.txt", true))
        {         
          foreach (DictionaryEntry entrywithchanges in changeHt)
          {
            changefile.WriteLine("Change are:");
            changefile.WriteLine(entrywithchanges.Key);
            changefile.WriteLine(entrywithchanges.Value);
          }
        }
      }     
    }
  }
}