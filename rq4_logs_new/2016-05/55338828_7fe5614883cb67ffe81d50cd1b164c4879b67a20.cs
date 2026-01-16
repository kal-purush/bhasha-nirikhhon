using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SimSharp.Samples
{
    class patientManager
    {
        private SystemRandom random = new SystemRandom();
        private static patientManager instance = null;
        private static bool patientsExists=false;
        public bool stillPatientsLeft()//Check if there are still patients in the List
        {
            return (patientsExists && (patients.Count > 0));
        }
        public static patientManager getInstance()//returns an instance of the patientManager
        {

            if (instance == null)
            {
                instance = new patientManager();
            }
            return instance;
        }
        private List<Patient> patients;//contains the remaining patients
        public List<Patient> createPatients(int amount)//fills the list of patients
        {
            this.patients = new PatientGenerator(amount).getPatientList();
            patientsExists = true;
            return this.patients;
        }
        public List<Patient> createPatients(List<Patient>p)//fills the list of patients
        {
            this.patients = p;
            patientsExists = true;
            return this.patients;
        }
        public List<Patient> getPatients(int amount=1,bool priority = true)//returns an amount of patients out of the list;amount has to be greater than 0
        {
            if(!patientsExists)
            {
                SystemRandom random = new SystemRandom();
                this.patients = createPatients(random.Next(5, 50));//if not defined, 5-50 patients get generated before using them
            }
            List<Patient> sublist = null;
            if (patients.Count>0)
            {
            if (amount>patients.Count)
            {
                amount = patients.Count;//sublist will get every remaining patient
            }
            sublist = patients.GetRange(0, amount);
            patients.RemoveRange(0, amount);
            if (priority)
            {
                priorize(sublist);
            }
            }
            return sublist;
        }
        public List<Patient> getRandomPatients(int min,int max, bool priority=true)//returns a random amount of patients out of the list
        {
            return getPatients(random.Next(min, max), priority);
        }
        public List<Patient> getAllPatients(bool priority = true)//returns all patients out of the list
        {
            return getPatients(patients.Count, priority);
        }
        private static int compareTTL(Patient x, Patient y)//needed for comparing
        {
            if (x.getTimeToLive() == null)
            {
                if (y.getTimeToLive() == null)
                {
                    // If x is null and y is null, they're
                    // equal. 
                    return 0;
                }
                else
                {
                    // If x is null and y is not null, y
                    // is greater. 
                    return -1;
                }
            }
            else
            {
                // If x is not null...
                //
                if (y.getTimeToLive() == null)
                // ...and y is null, x is greater.
                {
                    return 1;
                }
                else
                {
                    // ...and y is not null, compare the 
                    // lengths of the two strings.
                    //
                    int retval = x.getTimeToLive().CompareTo(y.getTimeToLive());

                    if (retval != 0)
                    {
                        // If the times are not equal,
                        // the longer is greater.
                        //
                        return retval;
                    }
                    else
                    {
                        // If the times are equal,
                        // sort them with ordinary comparison.
                        //
                        return x.getTimeToLive().CompareTo(y.getTimeToLive());
                    }
                }
            }
        }
        public void priorize(List<Patient> patients)//priorizese patients
        {
            patients.Sort(compareTTL);
        }
    }
}