
package graystar3server;

//Atomic masses in atomic mass units (amu. "mu")
//From CIAAW
//Atomic weights of the elements 2015 ciaaw.org/atomic-weights.htm, Aug. 2015
//Heaviest element treated in La (57)


public class AtomicMass {

    public static double getMass(String elName){

    double elMass = 1.0;  //default initialization

    if ("H".equals(elName)){
       elMass = 1.007;
        }
   
    if ("He".equals(elName)){
       elMass  = 4.002;
       }
   
    if ("Li".equals(elName)){
       elMass = 6.938;
      }
 
    if ("Be".equals(elName)){
       elMass  = 9.012;
  }
 
    if ("B".equals(elName)){
       elMass = 10.806;
   }
   
    if ("C".equals(elName)){
       elMass = 12.0096;
  }
   
    if ("N".equals(elName)){
       elMass = 14.006;
  }
 
    if ("O".equals(elName)){
       elMass = 15.999;
  }
 
    if ("F".equals(elName)){
       elMass = 18.998;
  }
 
    if ("Ne".equals(elName)){
       elMass  = 20.1797;
  }
 
    if ("Na".equals(elName)){
       elMass  = 22.989;
  }
 
    if ("Mg".equals(elName)){
       elMass  = 24.304;
  }
 
    if ("Al".equals(elName)){
       elMass  = 26.981;
  }
 
    if ("Si".equals(elName)){
       elMass  = 28.084;
  }
 
    if ("P".equals(elName)){
       elMass = 30.973;
  }
 
    if ("S".equals(elName)){
       elMass = 32.059;
 }
 
    if ("Cl".equals(elName)){
       elMass  = 35.446;
  }
 
    if ("Ar".equals(elName)){
       elMass  = 39.948;
  }
 
    if ("K".equals(elName)){
       elMass = 39.0983;
  }
 
    if ("Ca".equals(elName)){
       elMass  = 40.078;
  }
 
    if ("Sc".equals(elName)){
       elMass  = 44.955;
  }
 
    if ("Ti".equals(elName)){
       elMass  = 47.867;
  }
 
    if ("Va".equals(elName)){
       elMass  = 50.9415;
  }
 
    if ("Cr".equals(elName)){
       elMass  = 51.9961;
  }
 
    if ("Mn".equals(elName)){
       elMass  = 54.938;
  }
 
    if ("Fe".equals(elName)){
       elMass  = 55.845;
  }
 
    if ("Co".equals(elName)){
       elMass  = 58.933;
  }
 
    if ("Ni".equals(elName)){
       elMass  = 58.6934;
  }
 
    if ("Cu".equals(elName)){
       elMass  = 63.546;
  }
 
    if ("Zn".equals(elName)){
       elMass  = 65.38;
  }
 
    if ("Ga".equals(elName)){
       elMass  = 69.723;
  }
 
    if ("Ge".equals(elName)){
       elMass  = 72.630;
  }
 
    if ("As".equals(elName)){
       elMass  = 74.921;
  }
 
    if ("Se".equals(elName)){
       elMass  = 78.971;
  }
 
    if ("Br".equals(elName)){
       elMass  = 79.901;
  }
 
    if ("Kr".equals(elName)){
       elMass  = 83.798;
  }
 
    if ("Rb".equals(elName)){
       elMass  = 85.4678;
  }
 
    if ("Sr".equals(elName)){
       elMass  = 87.62;
  }
 
    if ("Y".equals(elName)){
       elMass = 88.905;
  }
 
    if ("Zr".equals(elName)){
       elMass  = 91.224;
  }
 
    if ("Nb".equals(elName)){
       elMass  = 92.906;
  }
 
    if ("Mo".equals(elName)){
       elMass  = 95.95;
  }
 
    if ("Ru".equals(elName)){
       elMass  = 101.07;
  }
 
    if ("Rh".equals(elName)){
       elMass  = 102.905;
  }
 
    if ("Pd".equals(elName)){
       elMass  = 106.42;
  }
 
    if ("Ag".equals(elName)){
       elMass  = 107.8682;
  }
 
    if ("Cd".equals(elName)){
       elMass  = 112.414;
  }
 
    if ("In".equals(elName)){
       elMass  = 114.818;
  }
 
    if ("Sn".equals(elName)){
       elMass  = 118.710;
  }
 
    if ("Sb".equals(elName)){
       elMass  = 121.760;
  }
 
    if ("Te".equals(elName)){
       elMass  = 127.60;
  }
 
    if ("I".equals(elName)){
       elMass = 126.904;
  }
 
    if ("Xe".equals(elName)){
       elMass  = 131.293;
  }
 
    if ("Cs".equals(elName)){
       elMass  = 132.905;
  }
 
    if ("Ba".equals(elName)){
       elMass  = 137.327;
  }
 
    if ("La".equals(elName)){
       elMass  = 138.905;
  }

// 
    return elMass; 


} // end of getMass method


} //end AtomicMass class