using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;

namespace JevLogin
{
    public sealed class ExampleLinq
    {
        private sealed class User
        {
            public string FirstName { get; }
            public string LastName { get; }
            public int Age { get; set; }

            public User(string firstName, string lastName)
            {
                FirstName = firstName;
                LastName = lastName;
            }
        }

        private readonly User[] _users;
        private readonly int[] _numbers;

        public ExampleLinq()
        {
            _users = new[]
            {
                new User("Roman", "Muratov") {Age = 18},
                new User("Ivan", "Petrov") {Age = 22},
                new User("Ilya", "Afanasyev") {Age = 18},
                new User("Igor", "Ivanov") {Age = 25},
                new User("Lera", "Muratova") {Age = 17},
                new User("Sveta", "Petrova") {Age = 27},
                new User("Lena", "Ivanova") {Age = 33},
                new User("Lera", "Muratova") {Age = 17},
                new User("Sveta", "Petrova") {Age = 27},
                new User("Lena", "Ivanova") {Age = 33}
            };
            _numbers = new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        }

        public void Filtration()
        {
            IEnumerable<int> evens = from i in _numbers
                                     where i % 2 == 0 && i > 10
                                     select i;

            IEnumerable<int> evens1 = _numbers.Where(i => i % 2 == 0 && i > 10);

            foreach (var i in evens)
            {
                Debug.Log(i);
            }
        }

        public void SelectComplexObjects()
        {
            var selectedUsers = from user in _users
                                where user.Age > 28
                                select user;

            var selectedUsers1 = _users.Where(u => u.Age > 28).ToList();

            foreach (var user in selectedUsers)
            {
                Debug.Log($"{user.FirstName} - {user.Age}");
            }
        }
    }
}