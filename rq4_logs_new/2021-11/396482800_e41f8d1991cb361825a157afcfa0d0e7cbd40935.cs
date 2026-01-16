using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeworkSerg.General
{
    class Homework1_Variables
    {
        public static void Start()
        {
            Console.WriteLine("- Пошли... Не стоит заставлять богов дожидатся...");
            Console.WriteLine();

            Console.WriteLine("- ВНИМАНИЕ!!! КОГДА ВЫЗОВЕМ - ДЕЛАЙТЕ ШАГ ВПЕРЕД ПО ОДНОМУ!");
            Console.WriteLine();

            Console.WriteLine("- Ох и любят имперцы все делать по списку...");
            Console.WriteLine();

            Console.WriteLine("- УЛЬФРИК БУРИВЕСНИК, ЯРЛ ФИНДХЕЛЬМА!");
            Console.WriteLine("- РАЛОФ ИЗ РИВЕРВУДА!");
            Console.WriteLine("- ЛОКЕР ИЗ РОРИКСТЕДА!");
            Console.WriteLine("- Постойте ка... ЭЙ ТЫ! ШАГ ВПЕРЕД! Тебя нет в списке, как твое имя?");
            Console.WriteLine();

            string name = Console.ReadLine();
            Console.WriteLine();

            Console.WriteLine("- Ну и страшный же ты " + name + "... Не поймешь даже кто ты...");
            Console.WriteLine("- С какой ты провинции Тамриэля?");
            Console.WriteLine();

            string Land = Console.ReadLine();
            Console.WriteLine();

            Console.WriteLine("- С такой дыры как " + Land + "? Теперь понятно почему ты в таких лохмотьях...");
            Console.WriteLine("- Лишь Шаогорат поймет чем ты раньше занимался...");
            Console.WriteLine("- Ты был вором? Или несостоявшимся воином? Или поехавшим магом?");
            Console.WriteLine("- Или искателем приключений, но потом тебе попала стрела в колено!? - Хаха...");
            Console.WriteLine("- Какова твоя професия? Только не говори что повар...");
            Console.WriteLine();

            string Profession = Console.ReadLine();
            Console.WriteLine();

            Console.WriteLine("- Что ж " + name + " из " + Land + " который раньше был " + Profession + ", Приветствую в Скайриме!");
            Console.WriteLine("- Капитан? Что с ним делать? Его нет в списке...");
            Console.WriteLine();

            Console.WriteLine("- В БЕЗДНУ СПИСОК! ДАВАЙ ЕГО НА ПЛАХУ!");
            Console.WriteLine();

            Console.WriteLine("- Увидимся в обливионе " + name + " из " + Land + "");
        }
    }
}