using System;
using System.Collections.Generic;
using System.Text;
using static System.Console;

using HomeworkMargaret.Tools;

namespace HomeworkMargaret.OOP
{
    class RegistrationForm
    {
        public string name;
        public int awardForTheirHead;
        public string latestLocation;
        public string typeOfWork;
        public string condition;

        public static int counter;

        public RegistrationForm(string name, int awardForTheirHead, string latestLocation, string typeOfWork, string condition)
        {
            this.name = name;
            this.awardForTheirHead = awardForTheirHead;
            this.latestLocation = latestLocation;
            this.typeOfWork = typeOfWork;
            this.condition = condition;
        }
    }

    class Homework1_PandoraBadassesRegistration
    {
        private static RegistrationForm[] forms = new RegistrationForm[10];
        public static void Start()
        {

            CT.Print("Welcome to HYPERION accounting form.");
            CT.Print("Please, choose an option:");
            int usersChoice;
            do
            {
                CT.Space();
                usersChoice = CT.Menu(
                    "Register a new bandit",
                    "Show all bandits",
                    "Change bandit's status");
                CT.Space();

                switch (usersChoice)
                {
                    case 1:
                        BanditRegister();
                        break;

                    case 2:
                        ShowBandits();
                        break;

                    case 3:
                        ChangeBanditsStatus();
                        break;                                            

                    case 0:
                        WriteLine("Thank you for choosing Hyperion!");
                        break;

                    default:
                        CT.ErrorMsg("Jack's gonna fire you");
                        break;
                }

            } while (usersChoice != 0);
        }

        public static void BanditRegister()
        {
            string name = CT.EnterString("Name: ");
            int award = CT.EnterInt("Award for their head: ");
            string location = CT.EnterString("Place where they can be seen: ");
            string work = CT.EnterString("Occupation: ");
            string status = CT.EnterString("Status (alive or dead): ");

            RegistrationForm bandit = new RegistrationForm(name, award, location, work, status);
            for (int i = 0; i < forms.Length; i++)
            {
                if (forms[i] == null)
                {
                    forms[i] = bandit;
                    break;
                }
            }
        }

        public static void ShowBandits()
        {
            
        }

        public static void ChangeBanditsStatus()
        {
            int usersChoice = CT.EnterInt("Choose a bandit: ");
            for (int i = 0; i < forms.Length; i++)
            {
                forms[usersChoice] = RegistrationForm()
            }
        }
    }
}