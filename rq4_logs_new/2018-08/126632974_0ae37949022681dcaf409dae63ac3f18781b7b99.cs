using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lesson_18
{
    class Program
    {
        static void Main(string[] args)
        {
            // normal array
           // int[] arr_1 = new int[5];
           // int[] arr_2 = new int[] {1,2,3,4,5};
           // int[] giant_Array = GetGiantArray();
           // int el_1 = giant_Array[0];
           // int el_2 = giant_Array[126547];
           // int el_3 = giant_Array[9999996];

            //Linked list
            //LinkedList<int> linkedList = GetGiantLinkedList();
            //var found = linkedList.Find(9999999);
            //linkedList.AddLast(new LinkedListNode<int>(100000001));

            //ArrayList
            ArrayList arrayList = new ArrayList();
            arrayList.Add(12);
            arrayList.Add(13);
            arrayList.Add("string");
            var arrayListElement = arrayList[0];
            //int sum = 0;
            //foreach (var el in arrayList)
            //{
                //sum += (int) el;
            //}

            //Generic List
            List<int> list = new List<int>();
            list.Add(1);
            list.Add(2);
            list.AddRange(new int[]{3,4,5,6,7,8,9});
            Console.WriteLine("Лист из элементов");
            PrintList(list);
            var listElement = list[0];
            Console.WriteLine("Можно обращаться по индексу как в обычном массиве");
            Console.WriteLine($"list[0] = {listElement}");
            list.Remove(4);
            Console.WriteLine("Убрали 4 из листа");
            PrintList(list);
            list.RemoveAt(0);
            Console.WriteLine("Убрали элемент по нулевому индексу");
            PrintList(list);
            list.RemoveAll(c=>c>5);
            Console.WriteLine("Убрали все элементы больше 5");
            PrintList(list);

            //Generics generates classes per each instance
            Foo<int>.Bar++;
            Console.WriteLine("А теперь выведем на экран наше статическое поле");
            Console.WriteLine(Foo<double>.Bar);

            //Generics in methods
            Console.WriteLine("Дженерики в методах");
            CheckIfString("this is string");
            CheckIfString<int>(12);
            
           //Constraints
            IGenericInterface<ValidGeneric> validGeneric = null;
            //IGenericInterface<NonValidGeneric> nonValidGeneric = null; // не скомпилится
           // IGenericInterface<AnotherNonValidGeneric> anotherNonValidGeneric = null; // тоже не скомпилится

            CheckIfStringConstraint(new ValidGeneric());
            //CheckIfStringConstraint(new NonValidGeneric()); // не скомпилится
            //CheckIfStringConstraint(new AnotherNonValidGeneric(12)); // и это тоже

            // Ковариантность
            //IBank<DepositAccount> depositBank = new Bank();
            //depositBank.DoOperation();
    
            //IBank<Account> ordinaryBank = depositBank;
            //ordinaryBank.DoOperation();

            // Контрвариантнось
            Account account = new Account();
            IBank<Account> ordinaryBank = new Bank<Account>();
            ordinaryBank.DoOperation(account);

            DepositAccount depositAcc = new DepositAccount();
            IBank<DepositAccount> depositBank = ordinaryBank;
            depositBank.DoOperation(depositAcc);

            Console.WriteLine();
            Console.ReadKey();
        }
        // Вспомогательны метод генерящий огромный массив
        static int[] GetGiantArray()
        {
            return Enumerable.Range(0, 10000000).ToArray();
        }
        // Вспомогательный метод генерящий огромный связный список
        static LinkedList<int> GetGiantLinkedList()
        {
            var _result = new LinkedList<int>();
            for (int i = 0; i < 100000000; i++)
            {
                _result.AddLast(new LinkedListNode<int>(i));
            }

            return _result;

        }
        //Вспомогательный метод для печати листов
        static void PrintList<T>(List<T> list) 
        {
            var sb = new StringBuilder();
            foreach (var elem in list)
            {
                sb.AppendFormat("{0} ", elem);
            }
            Console.WriteLine(sb.ToString());
        }
        // Класс для теста подтверждающего что дженерики создают отдельный класс на каждый тип
        class Foo<T>
        {
            public static int Bar;
        }
        //Простой дженерик метод проверяющий что переданный объект строка
        static void CheckIfString<T>(T parameter)
        {
            Console.WriteLine(typeof(T) == typeof(string));
        }
        //Дженерик интерфейс с констрейнтами
        interface IGenericInterface<T> where T : class, IEnumerable<T>, IList<T>, new()
        {
            void Bar(T arg);
        }
        //Версия простого дженерик метода с констрейнтами
        static void CheckIfStringConstraint<T>(T parameter) where T : class, IEnumerable<T>, IList<T>, new()
        {
            Console.WriteLine(typeof(T) == typeof(string));
        }
        //Класс-болванка подходящая под констрейнт
        class ValidGeneric : IEnumerable<ValidGeneric>, IList<ValidGeneric>
        {
            public ValidGeneric this[int index] { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

            public int Count => throw new NotImplementedException();

            public bool IsReadOnly => throw new NotImplementedException();

            public void Add(ValidGeneric item)
            {
                throw new NotImplementedException();
            }

            public void Clear()
            {
                throw new NotImplementedException();
            }

            public bool Contains(ValidGeneric item)
            {
                throw new NotImplementedException();
            }

            public void CopyTo(ValidGeneric[] array, int arrayIndex)
            {
                throw new NotImplementedException();
            }

            public IEnumerator<ValidGeneric> GetEnumerator()
            {
                throw new NotImplementedException();
            }

            public int IndexOf(ValidGeneric item)
            {
                throw new NotImplementedException();
            }

            public void Insert(int index, ValidGeneric item)
            {
                throw new NotImplementedException();
            }

            public bool Remove(ValidGeneric item)
            {
                throw new NotImplementedException();
            }

            public void RemoveAt(int index)
            {
                throw new NotImplementedException();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                throw new NotImplementedException();
            }
        }
        //Структура-болванка не подходящая под констрейнт
        struct NonValidGeneric
        {
            
        }
        //Класс-болванка не подходящая под констрейнт
        class AnotherNonValidGeneric
        {
            private int _index;
            public AnotherNonValidGeneric(int i)
            {
                this._index = i;
            }
        }

        //Просто заготовки для примеров на ко/контрвариантность
        class Account
        {
            static Random rnd = new Random();

            public void DoTransfer()
            {
                int sum = rnd.Next(10, 120);
                Console.WriteLine("Клиент положил на счет {0} долларов", sum);
            }
        }
        class DepositAccount : Account
        {
        }
        //Ковариантность
        //interface IBank<out T> where T : Account
        //{
        //    T DoOperation();
        //}

        //class Bank : IBank<DepositAccount>
        //{
        //    public DepositAccount DoOperation()
        //    {
        //        DepositAccount acc = new DepositAccount();
        //        acc.DoTransfer();
        //        return acc;
        //    }
        //}
        //Контрвариантность
        interface IBank<in T> where T : Account
        {
            void DoOperation(T account);
        }

        class Bank<T> : IBank<T> where T : Account
        {
            public void DoOperation(T account)
            {
                account.DoTransfer();
            }
        }
    }
}