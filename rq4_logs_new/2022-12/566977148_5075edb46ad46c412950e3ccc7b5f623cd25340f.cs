using System;

namespace CardCodedDate
{
    class Program
    {
        /// Короче, год делиться на знаки зодиака, они делятся на 4 масти по 8-9 карт от 6 до хз
        /// ♑♒♓♈♉♊♋♌♍♎♏♐ ♠♥♣♦ 678910JQKA
        /// Надо сделать конвертор из даты в шифр, и обратно
        
        static string mod = "change";

        static void Main(string[] args)
        {
            DateCalc.Inic();
            while (mod != "exit")
            {
                switch (mod)
                {
                    case "change":
                        ChangeMod();
                        break;
                    case "code":
                        CodeMod();
                        mod = "change";
                        break;
                    case "decode":
                        Console.WriteLine("Функционал отсутствует");
                        ChangeMod();
                        break;
                }
            }
        }

        static void ChangeMod()
        {
            Console.Write(
                "----------------------------------\n" +
                "Введите номер желаемого действия\n\n" +
                "№ | Обозначение\n" +
                "1 | Кодирование даты\n" +
                "2 | Расшифровка кода\n" +
                "3 | Выйти\n\n" +
                "Ввод: "
                );
            string a = Console.ReadLine();
            Console.WriteLine("----------------------------------");

            switch (a)
            {
                case "1":
                    mod = "code";
                    break;
                case "2":
                    mod = "decode";
                    break;
                case "3":
                    mod = "exit";
                    break;
                default:
                    Console.WriteLine("Команда не распознана");
                    break;
            }
        }

        static void CodeMod()
        {
            int month = 0;
            int day = 0;

            Console.Write("Номер месяца: ");
            month = Convert.ToInt32(Console.ReadLine());
            Console.Write("Номер дня: ");
            day = Convert.ToInt32(Console.ReadLine());
            string answer = DateCalc.DateCoder(month, day);
            if (answer == "-1")
            {
                return;
            }
            Console.WriteLine(answer);
        }

        
    }

    public class DateCalc
    {
        static string[] ZStr = new string[] { "Козерог", "Водолей", "Рыбы", "Овен", "Телец", "Близнецы", "Рак", "Лев", "Дева", "Весы", "Скорпион", "Стрелец" };
        static char[] MChar = new char[] { '♥', '♠', '♦', '♣' };
        static string[] CChar = new string[] { "6", "7", "8", "9", "10", "J", "Q", "K", "A" };

        static int[,] zodiac = new int[12, 2]; // 12 зодиаков, месяц и дата начала. Например [0,0]=12; [0,1]=22

        static int[] ZHowLong = new int[] { 29, 30, 30, 31, 31, 31, 31, 30, 33, 30, 30, 30 };
        static int[] MaxDayInMounth = new int[] { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

        public static string DateCoder(int month, int day)
        {
            string answer = "Это: ";

            if (month < 1 || month > 12)
            {
                Console.WriteLine("Недопустимое значение");
                return "-1";
            }
            if (day < 1 || day > MaxDayInMounth[month - 1])
            {
                Console.WriteLine("Недопустимое значение");
                return "-1";
            }

            int AnswZ = 0;
            int AnswM = 0;
            int AnswC = 0;

            for (int q = 0; q < 13; q++)
            {
                if (zodiac[q, 0] == month)
                {
                    if (zodiac[q, 1] <= day)
                    {
                        AnswZ = q;
                        q = 13;
                    }
                    else
                    {
                        AnswZ = q - 1;
                        q = 13;
                    }
                }
            }
            if (AnswZ == -1) { AnswZ = 11; }

            answer += ZStr[AnswZ];

            int numOfDay = 0;
            if (day >= zodiac[AnswZ, 1] && month == zodiac[AnswZ, 0])
            {
                numOfDay = day - zodiac[AnswZ, 1] + 1;
            }
            else
            {
                if (month == 1)
                {
                    numOfDay = MaxDayInMounth[11] - zodiac[AnswZ, 1] + day + 1;

                }
                else
                {
                    numOfDay = MaxDayInMounth[month - 2] - zodiac[AnswZ, 1] + day + 1;
                }
            }

            AnswM = numOfDay / ((ZHowLong[AnswZ] / 4) + 1);
            AnswC = numOfDay % ((ZHowLong[AnswZ] / 4) + 1);
            if (AnswC == 0)
            {
                AnswM--;
                AnswC = ((numOfDay - 1) % ((ZHowLong[AnswZ] / 4) + 1)) + 1;
            }
            answer += " " + MChar[AnswM];
            answer += " " + CChar[AnswC - 1];

            return answer;
        }

        public static void Inic()
        {
            for (int q = 0; q < 13; q++)
            {
                switch (q)
                {
                    case 0:         //Козерог
                        zodiac[q, 0] = 12;
                        zodiac[q, 1] = 23;
                        break;
                    case 1:         //Водолей
                        zodiac[q, 0] = 1;
                        zodiac[q, 1] = 21;
                        break;
                    case 2:         //Рыбы
                        zodiac[q, 0] = 2;
                        zodiac[q, 1] = 20;
                        break;
                    case 3:         //Овен
                        zodiac[q, 0] = 3;
                        zodiac[q, 1] = 21;
                        break;
                    case 4:         //Телец
                        zodiac[q, 0] = 4;
                        zodiac[q, 1] = 21;
                        break;
                    case 5:         //Близнецы
                        zodiac[q, 0] = 5;
                        zodiac[q, 1] = 22;
                        break;
                    case 6:         //Рак
                        zodiac[q, 0] = 6;
                        zodiac[q, 1] = 22;
                        break;
                    case 7:         //Лев
                        zodiac[q, 0] = 7;
                        zodiac[q, 1] = 23;
                        break;
                    case 8:         //Дева
                        zodiac[q, 0] = 8;
                        zodiac[q, 1] = 22;
                        break;
                    case 9:         //Весы
                        zodiac[q, 0] = 9;
                        zodiac[q, 1] = 24;
                        break;
                    case 10:        //Скорпион
                        zodiac[q, 0] = 10;
                        zodiac[q, 1] = 24;
                        break;
                    case 11:        //Стрелец
                        zodiac[q, 0] = 11;
                        zodiac[q, 1] = 23;
                        break;
                }
            }
            //Console.WriteLine("Весы стартуют: " + zodiac[9, 1] + "." + zodiac[9, 0]);
        }
    }

}