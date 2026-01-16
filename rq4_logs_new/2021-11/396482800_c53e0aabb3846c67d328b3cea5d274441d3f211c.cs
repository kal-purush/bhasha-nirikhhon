using System;
using System.Collections.Generic;
using System.Text;
using static System.Console;
using static HomeworkDenis.Tools.CH;

namespace HomeworkDenis.OOP.Homework1
{
    internal class Homework1_Classes
    {
        public static void Start()
        {
            var psychosList = new Psychos_List();
            int choice = 0;
            do
            {
                choice = Menu();
                switch (choice)
                {
                    case 1:
                        psychosList.Register_A_New_Psycho();
                        break;

                    case 2:
                        psychosList.Show_All_Psychos();
                        break;

                    case 3:
                        psychosList.Change_The_Status_Of_The_Psycho();
                        break;

                    case 4:
                        WriteLine("*отправился исследовать пучины космоса...*");
                        return;

                    default:
                        ErrorMsg("Baka! Выбери что-то одно из списка! ;N");
                        Print2Space();
                        break;
                }
            } while (choice != 4);
        }
        private static int Menu()
        {
            int choice = 0;
            WriteLine("Меню: ");
            WriteLine("1) Зарегистрировать нового сказочного долбаёба =3");
            WriteLine("2) Отобразить всех ебанатиков");
            WriteLine("3) Изменить статус жителя");
            WriteLine("4) Вылететь в космос =3");
            choice = EnterInt("Что бы вы хотели сделать?(мой вам совет: вылетайте нахер в космос через окно ;) ): ");
            WriteLine();
            return choice;
        }
    }
}