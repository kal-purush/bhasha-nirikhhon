using Commivoyajer_Core.Models;

namespace Commivoyajer_Core.Methods
{
    public class BruteForceMethod
    {
        public Output GetBruteForceMethod(double[,] dis_matrix) // Сама функция перебора
        {
            int[] city_array = new int[dis_matrix.GetLength(0) - 1];
            int[] answer_array = new int[dis_matrix.GetLength(0) - 1];
            double minimal_dist = 0;
            double new_dist = 0;

            for (int i = 0; i < dis_matrix.GetLength(0) - 1; i++)
                city_array[i] = i + 1;

            answer_array = city_array;
            minimal_dist = CalculateDistance(dis_matrix, city_array);

            while (permNxt(city_array, 0))
            {
                new_dist = CalculateDistance(dis_matrix, city_array);
                if (minimal_dist > new_dist)
                {
                    minimal_dist = new_dist;
                    answer_array = city_array;
                }
            }

            // Вывод пути и длины пути в текстбокс
            // answer_array - is answer
            return new Output
            {
                Sequence = answer_array,
                JourneyLength = 0,
                CalculationTime = 0,
                variantsCount = 0
            };
        }

        private int[] Swap(int[] ar, int i, int j) // доп.функция генерации перестановок маршрута
        {
            int a = ar[i];
            ar[i] = ar[j];
            ar[j] = a;
            return ar;
        }

        private bool permNxt(int[] ar, int a) // доп.функция генерации маршрута
        {
            int rt = ar.Length - 1;
            int i = rt - 1;
            int j = rt;

            while ((i >= a) && (ar[i] >= ar[i + 1]))
                i--;

            if (i < a)
                return false;

            j = rt;

            while (ar[i] >= ar[j])
                j--;

            ar = Swap(ar, i, j);
            a = i + 1;

            while (a < rt)
                ar = Swap(ar, a++, rt--);

            return true;
        }

        private double CalculateDistance(double[,] dis_matrix, int[] order) // доп.функция вычисления длины пути по маршруту
        {
            double dist = 0;
            int i;

            for (i = 0; i <= order.Length; i++)
            {
                if (i == 0)
                {
                    dist += dis_matrix[0, order[i]];
                }
                else if (i > 0 && i < order.Length)
                {
                    dist += dis_matrix[order[i], order[i - 1]];
                }
                else if (i == order.Length)
                {
                    dist += dis_matrix[order[i - 1], 0];
                }
            }
            return dist;
        }
    }
}