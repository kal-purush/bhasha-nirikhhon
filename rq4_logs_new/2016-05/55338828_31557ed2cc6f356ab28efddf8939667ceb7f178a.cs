using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;

namespace SimSharp.Samples
{
    //Definition of Patient status based on remaining TTL (in Seconds)
    public static class TTLGlobal
    {
        public const int TTL_DEAD = 0;                          //0 seconds left - dead
        public const int TTL_HOPELESS = 3600;                   //3600 seconds left - hopeless
        public const int TTL_SEVERELY_INJURED = 36000;          //36000 seconds left - severely injured
        public const int TTL_SLIGHTLY_INJURED = 300000000;      //>36000 seconds left - slightly injured
    }

    class PatientGenerator : IEnumerable, IEnumerator //PatientGenerator
    {
        /*          Fields          */
        private static int sequence = 0;                                //iterating number for KID
        public SystemRandom globalTime = new SystemRandom();
        private static List<Patient> patientList = new List<Patient>(); //List of all Patients
        private static int position = -1;                               //position in List for Iterator
        private Random randGen = new Random();                          //source for all randomness happening

        //Distribution Array
        //does not need to be given in percentages.Could also be set as:
        // # {190, 150, 50, 60 }
        // # {0.5 , 0.2, 12, 17} 
        //Chance for generating:         Position:
        private double[] distribution = { 0.25           // # dead Patient                  0
                                        , 0.25           // # hopeless Patient              1
                                        , 0.25           // # severly injured Patient       2
                                        , 0.25 };        // # slightly injured Patient      3

        /*          Constructor          */
        public PatientGenerator()
        {
            Console.WriteLine("Catastrophe engages\n");

        }
        //Creates PatientGenerator with given given number of causualities
        public PatientGenerator(int _numberOfCausualities)
        {
            generateCausualities(_numberOfCausualities);
            Console.WriteLine(_numberOfCausualities + " involved in catastrophe\n");
        }

        /*          Private         */
        //Produces given number of Patients and appends them to patientList
        private void generateCausualities(int _numberOfPatients)
        {
            for (int i = 0; i < _numberOfPatients; i++)
            {
                addPatient();
            }
        }
        /*          Public          */

        /*public TimeSpan get_random_time()
        {
            TimeSpan random_time = new TimeSpan(globalTime.Next(0,12));
            return random_time;
        }*/

        //Adds a new Patient to the List according to the given distribution
        public bool addPatient()
        {
            double x = randGen.NextDouble();            // determins the Patients state
            double sumDist = distribution.Sum();

            if (x <= distribution[0] / sumDist)
            {
                //Console.WriteLine("dead Patient created");
                patientList.Add(new Patient(TTLGlobal.TTL_DEAD));
            }
            else if (x <= (distribution[0] + distribution[1]) / sumDist)
            {
                //Console.WriteLine("hopeless Patient created");
                patientList.Add(new Patient((double)randGen.Next(TTLGlobal.TTL_DEAD + 1, TTLGlobal.TTL_HOPELESS))); // Creates hopeless patient as distributed
            }
            else if (x <= (distribution[0] + distribution[1] + distribution[2]) / sumDist)
            {
                //Console.WriteLine("severly injured Patient created");
                patientList.Add(new Patient((double)randGen.Next(TTLGlobal.TTL_HOPELESS + 1, TTLGlobal.TTL_SEVERELY_INJURED)));
            }
            else if (x <= distribution.Sum())
            {
                //Console.WriteLine("slightly injured Patient created");
                patientList.Add(new Patient((double)randGen.Next(TTLGlobal.TTL_SEVERELY_INJURED + 1, TTLGlobal.TTL_SLIGHTLY_INJURED)));
            }
            else
                Console.WriteLine("Something went wrong!");

            return true;
        }

        public static int get_new_id()
        {
            return ++sequence;
        }

        public IEnumerator GetEnumerator()
        {
            return (IEnumerator)this;
        }

        public bool MoveNext()
        {
            if (position < patientList.Count - 1)
            {
                ++position;

                return true;
            }
            return false;
        }


        public void Reset()
        {
            position = -1;
        }

        public object Current
        {
            get
            {
                return patientList[position];
            }
        }

        public static object getNextPatient()
        {
            return patientList[++position];
        }
        public List<Patient> getPatientList()
        {
            return patientList;
        }

        //changes the distribution
        public void setDistribution(double dead, double hopeless, double severe, double slight)
        {
            distribution[0] = dead;
            distribution[1] = hopeless;
            distribution[2] = severe;
            distribution[3] = slight;
        }
    }


    class Patient
    {

        private string KID = "4 KH 12 " + PatientGenerator.get_new_id();
        public DateTime arrivalTime;
        private DateTime TTL = new DateTime(); //Is the time the Patient has left to live
        private int triageNr;


        //When creating the patient, TTL will be set to the current time + the given seconds
        public Patient(double seconds)
        {
            DateTime thisDay = DateTime.Now.AddSeconds(seconds);
            setTimeToLive(new DateTime(thisDay.Year, thisDay.Month, thisDay.Day, thisDay.Hour, thisDay.Minute, thisDay.Second));
            Console.WriteLine("TTL: " + thisDay.ToString());
            //Console.WriteLine("Patient with life expectancy:" + TTL + "created");
            //thisDay.AddSeconds(150);
        }

        public String getKID()
        {
            return KID;
        }

        public DateTime getTimeToLive()
        {
            return TTL;
        }
        public String getTimeToLiveString()
        {
            return TTL.ToLongTimeString();
        }
        public void setTimeToLive(DateTime _TTL)
        {
            TTL = _TTL;
        }

        public int getTriageNr()
        {
            return triageNr;
        }

        public void setTriageNr(int trnr)
        {
            triageNr = trnr;
        }


        public void triagePatient(DateTime TTL)//changes of this method, has to be also changed in Priority class
        // 1 slightly injured, 2 severely injured, 3 hopeless, 4 dead
        // TTL in seconds
        {

            TimeSpan timeDifference = TTL.Subtract(DateTime.Now);
            double TTLInSec = timeDifference.TotalSeconds;
            //Console.WriteLine("calculated time to Live" + TTLInSec + "| given TTL:" + TTL + "| given TTL:" + TTL.Second); //for debuging purposes
            if (TTLInSec <= TTLGlobal.TTL_DEAD) triageNr = 4; //dead
            else if (TTLInSec > TTLGlobal.TTL_DEAD && TTLInSec <= TTLGlobal.TTL_HOPELESS) triageNr = 3; //TTL <= 60min -> hopeless
            else if (TTLInSec > TTLGlobal.TTL_HOPELESS && TTLInSec <= TTLGlobal.TTL_SEVERELY_INJURED) triageNr = 2; //TTL > 15min and <= 10h -> serverely injured
            else triageNr = 1; //TTL > 10h -> slightly injured

        }

        public void withdrawTTL(TimeSpan subtrahend)
        {
            TTL -= subtrahend;
        }
    }




    class Program
    {
        static void Main()
        {
            //Patient Patient1 = new Patient(100);

            //PatientGenerator catastrophe = new PatientGenerator(10);

            //PatientGenerator.getNextPatient();
            //PatientGenerator.getNextPatient();
            //PatientGenerator.getNextPatient();
        }
    }


}