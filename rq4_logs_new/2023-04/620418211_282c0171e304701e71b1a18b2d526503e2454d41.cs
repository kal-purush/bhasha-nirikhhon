using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Home_6
{
    /*
    Создать программу для имитации работы клиники. (Clinic)
Пусть в клинике будет три врача: офтальмолог (ophthalmologist), терапевт (therapist) и стоматолог (dentist).  
Каждый врач имеет метод «лечить» Treat(), но каждый врач лечит по-своему. Так же предусмотреть класс «Пациент»(Patient) и enum «IlnessType» (Eyes, Teeth, Other) - (тип болезни, что человека беспокоит)
Создать объект класса «Пациент». 
Так же создать метод в классе Клиника, который будет отправлять пациента к доктору. Далее доктор должен лечить пациента. 
Если план лечения == Eyes – назначить офтальмолога и выполнить метод  лечить.  
Если план лечения == Teeth – назначить стоматолог и выполнить метод  лечить.  
Если план лечения == Other – назначить терапевта и выполнить метод лечить.
    */
    public class Clinic
    {
        static void Main(string[] args)
        {
            ShowInformation showInformation = new ShowInformation();
            PersonsCreator personsCreator = new PersonsCreator();

            ///create some doctors
            Doctor[] doctors = personsCreator.DoctorsCreator();

            /// maybe a bad decision, I can move the logic to another class, 
            ///but i wanted to see how i can call the method of this Doctor class without creating a new data
             Doctor doctor = doctors[0]; 

            /// create some patients
            Patient[] patients = personsCreator.PatientCreator();
            showInformation.ShowPatients(patients);

            ///lets treating
            patients = doctor.Treat(patients);

            /// show patient information
            Console.WriteLine("_________________________________________________");
            Console.WriteLine("\n\nInformation about patients after Treating\n\n");
            Console.WriteLine("_________________________________________________");
            showInformation.ShowPatients(patients);
            
            Console.ReadLine();
        }
    }
}