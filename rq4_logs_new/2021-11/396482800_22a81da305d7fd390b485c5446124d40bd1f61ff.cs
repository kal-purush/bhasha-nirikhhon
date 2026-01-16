using System;
using System.Collections.Generic;
using System.Text;

namespace HomeworkDmitriy.General
{
    internal class Homework1_Variables
    {
        public static void Startuem()
        {
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine("           << Станция тихого и не приметного городка постепенно наполнялась шумом приближающегося поезда, который направлялся из дальних земель Уэст-Элизабет. >>           ");
            Console.WriteLine("           << Стук колёс и клубы дыма давали местным жителям знать что через несколько мгновений городок наполнится новоприбывшими. >>           ");
            Console.WriteLine();

            Console.WriteLine("           Поезд останавливается и вы выходите из него...           ");  //Это был жаркий день, солнце находилось в зените, мало кто вообще выходил в это пекло, но посетить салун для каждого - традиция... не изменяемая годами..
            Console.WriteLine();

            Console.WriteLine("           К вам подбегает незнакомец:           ");
            Console.WriteLine("           - Эй ты! Да, я к тебе обращаюсь! Как твоё имя?           ");
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();

                    string name = Console.ReadLine();
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();

            Console.WriteLine("           Что ж " + name+ ", таких имён я давненько не слыхал... Ну ладно..           ");
            Console.WriteLine("           Пойдём, поможешь мне расправится с двумя негодяями! Ответ нет, не принимается, иначе станешь одним из них!           ");
            Console.WriteLine();

            Console.WriteLine("           Вы идёте по небольшому городку который больше похож на пустыню нежели на город...           ");
            Console.WriteLine("           Заходите в месный салун с незнакомцем, и он предлагает вам выпить:           ");
            Console.WriteLine();

            Console.WriteLine("           - Эй, " + name+ " будешь пить?");
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();

                    Console.ReadLine();
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();

            Console.WriteLine("           - Аааа, и зачем я тебя спрашиваю! Я в любом случае выпью!           ");
            Console.WriteLine();
            Console.WriteLine("           - Давай заканчиваем и разберёмся с негодяями, вон они сидят у дальнего столика!           ");
            Console.WriteLine("           - Сколько патронов в твоём револьвере?           ");
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();

                    string patron = Console.ReadLine();
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();

            Console.WriteLine("           Нормалды, нам с тобой хватит " + patron + " патронов что б разобратся с ними! Идём!           ");


            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();
        }
    }
}