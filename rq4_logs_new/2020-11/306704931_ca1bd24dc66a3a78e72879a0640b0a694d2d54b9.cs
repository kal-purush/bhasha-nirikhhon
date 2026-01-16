using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;


namespace JevLogin
{
    public sealed class ExampleLinkedList : MonoBehaviour
    {
        private sealed class User
        {
            #region Fields

            public string FirstName { get; }
            public string LastName { get; }

            #endregion


            #region Properties

            public User(string firstName, string lastName)
            {
                FirstName = firstName;
                LastName = lastName;
            }

            #endregion
        }

        public void Main()
        {
            LinkedList<int> numbers = new LinkedList<int>();
            numbers.AddLast(1);     // вставляем узел со значением 1 на последнее место
                                    // так как в списке нет узлов, то последнее будет также и первым
            numbers.AddFirst(2);    // вставляем узел со значением 2 на первое место
            numbers.AddAfter(numbers.Last, 3);  // вставляем после последнего узла новый узел со значением 3
                                                // теперь у нас список имеет следующую последовательность: 2, 1, 3
            foreach (int i in numbers)
            {
                Debug.Log(i);
            }

            LinkedList<User> persons = new LinkedList<User>();
            // добавляем persona в список и получим объект LinkedListNode<Person>, в котором хранится имя Tom

            LinkedListNode<User> users = persons.AddLast(new User("Lera", "Petrova"));

            persons.AddLast(new User("Igor", "Ivanov"));
            persons.AddFirst(new User("Ilya", "Petrov"));

            Debug.Log(users.Previous.Value.FirstName);  // получаем узел перед томом и его значение
            Debug.Log(users.Next.Value.LastName);       // получаем узел после тома и его значение
        }
    }
}