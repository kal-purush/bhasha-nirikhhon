using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;


namespace JevLogin
{
    public sealed class ExamplePredicate
    {
        public void Main()
        {
            TraditionalDelegateSyntax();
        }

        private void TraditionalDelegateSyntax()
        {
            // Создать список целых чисел
            List<int> list = new List<int>();
            list.AddRange(new int[] { 20, 1, 4, 8, 9, 4, 4 });
            // Вызов FindAll() с использованием традиционного синтаксиса делегатов
            // Создаем обобщенный экземпляр обобщенного делегата, используя встроенный делегат Predicate
            Predicate<int> predicate = new Predicate<int>(IsEvenNumber);
            // Создаем список целых чисел, используя метод FindAll, в который передаем делегат
            List<int> evenNumbers = list.FindAll(predicate);
            Debug.Log("Здесь только четные числа");
            foreach (int evenNumber in evenNumbers)
            {
                Debug.Log(evenNumber);
            }
        }

        private bool IsEvenNumber(int i)
        {
            return i % 2 == 0;
        }
    }
}