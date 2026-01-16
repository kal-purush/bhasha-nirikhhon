using System;
using System.Collections.Generic;

namespace PhoneBook
{
    class RandomContactListCreator
    {
        // Members
        private int listCount;
        private Random random = new Random();
        private List<string> myNames = new List<string>();

        // Men random parametrs
        private string[] menSurNames = new string[] {
            "Иванов","Петров","Бородинов","Кутузов","Самойлов","Албутов","Викторов","Шаров","Ушаков",
        };
        private string[] menForeNames = new string[] {
            "Алексей","Александр","Тимофей","Илья","Владимир","Виктор","Михаил","Петр","Николай"
        };
        private string[] menMiddleNames = new string[] {
            "Иванович","Александрович","Ильич","Владимирович","Петрович","Николаевич","Анатольевич",
            "Алексеевич","Константинович"
        };

        // Women random parametr
        private string[] womenSurNames = new string[] {
            "Ушакова","Иванова","Смирнова","Петрова","Шарова","Самойлова","Марженко","Потапова"
        };
        private string[] womenForeNames = new string[] {
            "Елена","Анжелика","Екатерина","Мария","Маргарита","Юлия","Анна","Елизавета"
        };
        private string[] womenMiddleNames = new string[] {
            "Ивановна","Александровна","Владимировна","Петровна","Николаевна","Анатольевна",
            "Алексеевна","Константиновна"
        };

        // Contructor
        public RandomContactListCreator() {
        }

        /// <summary>
        /// Create random contact list in second fotmat(Surname_Forename_Middlename_PhoneNumber)
        /// </summary>
        /// <param name="count"></param>
        /// <returns>COunt of contact list</returns>
        public List<string> CreateContactList(int count){
            Sex sex = 0;

            for (int i = 0; i < count; i++) {
                sex=(Sex)random.Next(2);

                if (sex == Sex.Female)
                {
                    myNames.Add(string.Format("{0} {1} {2} {3}",
                        womenSurNames[random.Next(womenSurNames.Length)],
                        womenForeNames[random.Next(womenForeNames.Length)],
                        womenMiddleNames[random.Next(womenMiddleNames.Length)],
                        GeneratePhoneNumber()));
                }
                else if (sex == Sex.Male) {
                    myNames.Add(string.Format("{0} {1} {2} {3}",
                        menSurNames[random.Next(womenSurNames.Length)],
                        menForeNames[random.Next(womenForeNames.Length)],
                        menMiddleNames[random.Next(womenMiddleNames.Length)],
                        GeneratePhoneNumber()));
                }
            }
            return myNames;
        }

        private string GeneratePhoneNumber() {   
            string phoneNumber = string.Format("8-{0}-{1}-{2}",random.Next(910,980),
                random.Next(456,999),random.Next(1234,6789));

            return phoneNumber;
        }
    }
}