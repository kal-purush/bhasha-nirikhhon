using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeWork
{
    public static class DZ7_dop
    {
        public static void HR_management()
        {
            //Создаем компании
            Company AudiCompany = new Company("Audi", 3);
            AudiCompany.ProfessionsList.Add("бухгалтер", false);
            AudiCompany.ProfessionsList.Add("водитель", false);
            AudiCompany.ProfessionsList.Add("слесарь", false);

            Company MersedesCompany = new Company("Mersedes", 1);
            MersedesCompany.ProfessionsList.Add("слесарь", false);

            Company VolksWagenCompany = new Company("VolksWagen", 2);
            VolksWagenCompany.ProfessionsList.Add("закупщик", false);
            VolksWagenCompany.ProfessionsList.Add("слесарь", false);

            Company GMCCompany = new Company("GMC", 2);
            GMCCompany.ProfessionsList.Add("продавец", false);
            GMCCompany.ProfessionsList.Add("слесарь", false);


            List<Company> companyList = new List<Company>();

            companyList.Add(AudiCompany);
            companyList.Add(MersedesCompany);
            companyList.Add(VolksWagenCompany);
            companyList.Add(GMCCompany);


            List<Person> workersList = new List<Person>();
            //создаем безработных
            Person per1 = new Person("Пяточкин В.", "бухгалтер");
            workersList.Add(per1);
            Person per2 = new Person("Ласточкин И.", "продавец");
            workersList.Add(per2);
            Person per3 = new Person("Пятеркин С.", "закупщик");
            workersList.Add(per3);
            Person per4 = new Person("Гаврилов А.", "слесарь");
            workersList.Add(per4);
            Person per5 = new Person("Иванов В.", "слесарь");
            workersList.Add(per5);
            Person per6 = new Person("Петров Е.", "слесарь");
            workersList.Add(per6);
            Person per7 = new Person("Сидоров Ю.", "слесарь");
            workersList.Add(per7);
            Person per8 = new Person("Цветаев В.", "слесарь");
            workersList.Add(per8);



            //создание HR менеджера
            HR_Manager HR_man = new HR_Manager("Иванова О.К.");

            //подобрать безработных в компании
            HR_man.ArrangePeronsByProfession(workersList, companyList);
        }

    }

    class Person
    {
        private string name;
        private string profession;

        public bool isEmlpoyed;

        public string Name { get => name; set => name = value; }
        public string Profession { get => profession; set => profession = value; }

        public Person(string personName, string prof)
        {
            name = personName;

            profession = prof;

            isEmlpoyed = false;

            Console.WriteLine("Добавлен человек в базу безработных: {0}, специальность: {1}", Name, Profession);
        }
    }

    class Company
    {
        public int capacityWorkers;

        public bool workersIsNeeded;

        public string companyName;

        public Dictionary<string, bool> ProfessionsList; //название професии, потребность

        public Dictionary<Person, string> EmployeesList; //нанятые сотрудники компании

        public Company(string name, int capacity)
        {
            companyName = name;
            capacityWorkers = capacity;
            ProfessionsList = new Dictionary<string, bool>();
            EmployeesList = new Dictionary<Person, string>();
        }

        public void HirePerson(Person worker, string profession)
        {
            if (EmployeesList.Count < capacityWorkers)
            {
                EmployeesList.Add(worker, profession);
                worker.isEmlpoyed = true;
                Console.WriteLine(worker.Name + " устроен на работу в компанию: " + companyName + "\n по профессии: " + profession + "\n");
            }
            else workersIsNeeded = false;
        }
    }


    class HR_Manager
    {
        string name;
        //int salary;

        public string Name { get => name; set => name = value; }

        public HR_Manager(string personName)
        {
            name = personName;
        }

        public List<Person> FindWorkersByProfession(List<Person> workers, string profession)
        {
            var result = workers.Where(p => p.Profession.Contains(profession) & p.isEmlpoyed == false);

            List<Person> workersList = new List<Person>();

            foreach (var item in result)
            {
                workersList.Add(item);
            }

            return workersList;
        }

        public void ArrangePeronsByProfession(List<Person> workersList, List<Company> companyList)
        {
            foreach (Company company in companyList) //перебор компаний
            {
                foreach (var profession in company.ProfessionsList) //перебр профессий компании
                {
                    if (profession.Value == false) //флаг что сотридники по этой профессии еще нужны
                    {
                        List<Person> workers = FindWorkersByProfession(workersList, profession.Key);

                        if (workers.Count > 0)
                        {
                            company.HirePerson(workers[0], profession.Key);
                        }
                    }
                }
            }
        }


    }
}