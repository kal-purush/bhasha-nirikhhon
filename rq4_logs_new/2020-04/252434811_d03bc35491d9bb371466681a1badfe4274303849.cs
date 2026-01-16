using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeWork
{
    class DZ8_dop
    {
        public static void Start()
        {
            string str = "ЗаполнитьЗначенияСвойств(НовыйЗаказ, МассивЗаказов[0],,'Номер,Ответственный,Проведен');";
            Console.WriteLine("Исходная строка:\n" + str);
            bool isBalanced = Balance(str);
            Console.WriteLine($"В данной строке {(isBalanced ? "" : "не")} соблюдается баланс скобок\n");
        }

        private static bool Balance(string str)
        {
            bool isBalanced = true;

            //массив для поиска в строке
            var arrOpened = new[] { '(', '{', '[' };
            var arrClosed = new[] { ')', '}', ']' };

            //служит для сравнения парных скобок
            var parityQuotes = new Dictionary<char, char>()
            {
                { ')', '('},
                { '}', '{'},
                { ']', '['},
            };

            int startIndex = 0;
            int nextIndex = 0;

            //вначале вырезаем из строки парные скобки идущие подряд
            for (int i = 0; i < str.Length; i++)
            {
                nextIndex = str.IndexOfAny(arrOpened, startIndex);
                if (nextIndex >= 0)
                {
                    startIndex = nextIndex+1;
                    //ищем следущую парную скобку
                    int parIndex = str.IndexOfAny(arrClosed, startIndex);
                    if (str[nextIndex] == parityQuotes[str[parIndex]])
                    {
                        //чтоб потом они не мешались при сравнении заменяем из любым символом
                        str = str.Remove(nextIndex, 1).Insert(nextIndex, "_");
                        str = str.Remove(parIndex, 1).Insert(parIndex, "_");
                        startIndex = parIndex + 1;
                    }
                }
            }

            var openedIndecies = new List<char>();
            var closedIndecies = new List<char>();

            //составляем список открывающих скобок идущих подряд
            startIndex = 0;
            do
            {
                nextIndex = str.IndexOfAny(arrOpened, startIndex);
                if (nextIndex >= 0)
                {
                    openedIndecies.Add(str[nextIndex]);
                    startIndex = nextIndex+1;
                }
            } while (nextIndex != -1);

            //составляем список закрывающих скобок идущих подряд
            startIndex = 0;
            do
            {
                nextIndex = str.IndexOfAny(arrClosed, startIndex);
                if (nextIndex >= 0)
                {
                    closedIndecies.Add(str[nextIndex]);
                    startIndex = nextIndex + 1;
                }
            } while (nextIndex != -1);

            //если их разное количество - то неверно
            if (openedIndecies.Count != closedIndecies.Count)
            {
                isBalanced = false;
                return isBalanced;
            }

            //сравниваем
            for (int i = 0, j = openedIndecies.Count-1; i < openedIndecies.Count; i++, j--)
            {
                if (openedIndecies[j] != parityQuotes[closedIndecies[i]])
                {
                    break;
                }
            }

            return isBalanced;
        }
    }
}