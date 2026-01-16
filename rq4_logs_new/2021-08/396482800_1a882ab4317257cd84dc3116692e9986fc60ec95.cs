using System;

using static System.Console;

namespace HomeworkKarina.BasicThings
{
    class HW2_Opetaions
    {
		public static void Start()
		{
			int choice = -1;

			do
			{
				WriteLine("-----Меню-----");
				WriteLine("---1 - Задача---");
				WriteLine("---2 - Задача---");
				WriteLine("---3 - Задача---");
				WriteLine("---4 - Задача---");
				WriteLine("---5 - Задача---");
				WriteLine("---0 - Выход---");
				Write("-->");

				choice = Convert.ToInt32(Console.ReadLine());

				switch (choice)
				{
					case 1:
						Task1();
						break;
					case 2:
						Task2();
						break;
					case 3:
						Task3();
						break;
					case 4:
						Task4();
						break;
					case 5:
						Task5();
						break;
					case 0:
						WriteLine("Выход");
						break;

					default:
						WriteLine("Ошибка");
						break;
				}
			} while (choice != 0);
		}

		/// <summary>
		/// ЗАДАЧА №1. 
		/// Найти Х используя формулу дискриминанта.
		/// 
		///		ax^2 - bx - c = 0
		///		D = b^2-4*a*c
		///		x1,2 = -b + - корень D / 2*a
		/// </summary>
		public static void Task1()
		{
			

			WriteLine("1. Решить квадтратное уравнение ax^2 - bx - c = 0");

			int a;
			int b;
			int c;
			int D;
			int x1;
			int x2;

			WriteLine("Введите a");
			a = Convert.ToInt32(ReadLine());

			WriteLine("Введите b");
			b = Convert.ToInt32(ReadLine());

			WriteLine("Введите c");
			c = Convert.ToInt32(ReadLine());

			D = (int)(Math.Pow(b, 2) - 4 * a * c);

			if (D > 0 || D == 0)
			{
				x1 = (int)((-b - Math.Sqrt(D)) / (2 * a));
				x2 = (int)((-b + Math.Sqrt(D)) / (2 * a));

				WriteLine("x1={0} x2={1}", x1, x2);
				ReadLine();
			}
			else
			{
				WriteLine("Действительных корней нет");
				ReadLine();
			}
		}

		public static void Task2()
		{
			WriteLine("2.Вычичислить значение функции y=f(x): поправка");
			Write("Введите значени Х: ");
			int valueX = Convert.ToInt32(ReadLine());
			int x = valueX;
			int resultY;

			if (x > 0)
			{
				resultY = 2 * valueX - 10;
				WriteLine("x={0}, y={1}", x, resultY);
			}
			else if (x < 0)
			{
				resultY = 2 * Math.Abs(x) - 1;
				WriteLine("x={0}, y={1}", x, resultY);
			}
			else
			{
				resultY = 0;
				WriteLine("x={0}, y={1}", x, resultY);
			}

			ReadLine();
		}

		public static void Task3()
		{
			WriteLine("3. Максимальное число из трех");

			int a1;
			int b1;
			int c1;

			WriteLine("Введите a1");
			a1 = Convert.ToInt32(ReadLine());

			WriteLine("Введите b1");
			b1 = Convert.ToInt32(ReadLine());

			WriteLine("Введите c1");
			c1 = Convert.ToInt32(ReadLine());

			// маленький лайфхак, как сделать короче
			// int result = 0; 
			// result = Math.Max(a1, b1);
			// result = Math.Max(result, c1);

			if (a1 > b1 && a1 > c1)
			{
				WriteLine("{0} наибольшее число", a1);
			}
			else if (b1 > c1)
			{
				WriteLine("{0} наибольшее число", b1);
			}
			else
			{
				WriteLine("{0} наибольшее число", c1);
			}
			ReadLine();
		}

		public static void Task4()
		{
			WriteLine("4. Какой координатной четверти принадлежит точка?");

			int x;
			int y;

			WriteLine("Введите значение x");
			x = Convert.ToInt32(ReadLine());

			WriteLine("ведите значение y");
			y = Convert.ToInt32(ReadLine());

			if (x < 0 && y > 0)
			{
				WriteLine("Принадлежит первой четверти");
			}
			else if (x > 0 && y > 0)
			{
				WriteLine("Принадлежит второй четверти");
			}
			else if (x > 0 && y < 0)
			{
				WriteLine("Принадлежит третей четверти");
			}
			else if (x < 0 && y < 0)
			{
				WriteLine("Принадлежит четвертой четверти");
			}
			else if (x == 0 && y == 0)
			{
				WriteLine("Не пренадлежит ни одной четверти");
			}
			else if (x == 0 && y > 0)
			{
				WriteLine("Принадлежит и первой, и второй четверти");
			}
			else if (x > 0 && y == 0)
			{
				WriteLine("Принадлежит и второй, и третей четверти");
			}
			else if (x == 0 && y < 0)
			{
				WriteLine("Принадлежит и третей, и четвертой четверти");
			}
			else if (y == 0 && x < 0)
			{
				WriteLine("Принадлежит и первой, и четвертой четверти");
			}

			ReadLine();
		}

		public static void Task5()
		{
			WriteLine("5. Принадлежит ли число дипазону от 1 до 100");

			WriteLine("Введите число от 1 до 100");

			int n = Convert.ToInt32(ReadLine());
			if (n >= 1 && n <= 100)
			{
				WriteLine("Принадлежит");
			}
			else
			{
				WriteLine("Не пренадлежит");
			}

			ReadLine();
		}
	}
}