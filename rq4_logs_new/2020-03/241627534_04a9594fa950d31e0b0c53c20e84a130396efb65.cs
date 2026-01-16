using System;
using System.Collections.Generic;




class studentas 
{
  public string vardas {get; set;}
  public string pavarde {get; set;}
  public double rezEgz {get; set;}
  public double galutinis {get; set;}
  public double galutinisMed {get; set;}

  public List<double> balai = new List<double>();
  public double vid {get; set;} = 0;

  public studentas (string vardas, string pavarde, double rezEgz, List<double> balai)
  {
    this.vardas = vardas;
    this.pavarde = pavarde;
    this.rezEgz = rezEgz;
    this.balai = balai;
    foreach (var value in this.balai)
    {
      this.vid = this.vid + value;
    }
    this.vid = this.vid / this.balai.Count;
    this.galutinis = this.vid * 0.3 + this.rezEgz * 0.7;
    
    this.balai.Sort();
    this.galutinisMed = this.balai[this.balai.Count / 2] * 0.3 + this.rezEgz * 0.7;
  }


}