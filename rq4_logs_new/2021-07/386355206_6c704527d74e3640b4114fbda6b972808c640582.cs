using System;
using static Array.Tasks.ArrayElementsReverse;
using static Array.Tasks.ArrayElementsSequence;
using static Array.Tasks.EvenArrayElements;
using static Array.Tasks.EverySecondElement;
using static Array.Tasks.ExactElementStop;

namespace Array
{
    class Program
    {
        static void Main(string[] args)
        {
            int[] array = {0, 1, 2, 3, 4, 5, -4, -1, 10, 55};
            // Вам необходимо сделать несколько блоков кода, которые делают следующее:
            //
            // Блок 1. Выводит все элементы массива.
            
            AESequence(array);
            
            // Блок 2. Выводит все элементы массива в обратном порядке.
            
            AEReverse(array);
            
            // Блок 3. Выводит чётные элементы массива.
            
            EvenAE(array);
            
            // Блок 4. Выводит все элементы массива через 1.
            
            ESElement(array);
            
            // Блок 5. Выводит все элементы массива пока не встретится элемент -1.
            
            ElementStop(array);

        }
    }
}