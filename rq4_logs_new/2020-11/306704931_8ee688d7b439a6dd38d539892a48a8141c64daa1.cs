using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using UnityEngine;


namespace JevLogin
{
    public sealed class ExampleObservableCollection : MonoBehaviour
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
            ObservableCollection<User> users = new ObservableCollection<User>
            {
                new User("Roman", "Muratov"),
                new User("Evgeniy", "Loginov"),
                new User("Maksim", "Kuznetcov")
            };

            users.CollectionChanged += Users_CollectionChanged;

            users.Add(new User("Lera", "Petrova"));
            users.RemoveAt(1);
            users[0] = new User("Sveta", "Ivanova");

            foreach (var el in users)
            {
                Debug.Log(el.FirstName);
            }
        }

        private void Users_CollectionChanged(object sender, NotifyCollectionChangedEventArgs e)
        {
            switch (e.Action)
            {
                case NotifyCollectionChangedAction.Add:
                    User newUser = (User)e.NewItems[0];
                    Debug.Log($"Добавлен новый элемент: {newUser.FirstName}");
                    Debug.Log($"Added a new element: {newUser.FirstName}");
                    break;
                case NotifyCollectionChangedAction.Move:
                    break;
                case NotifyCollectionChangedAction.Remove:
                    User oldUser = (User)e.OldItems[0];
                    Debug.Log($"Removed object: {oldUser.FirstName}");
                    break;
                case NotifyCollectionChangedAction.Replace:
                    User replacedUser1 = (User)e.OldItems[0];
                    User replacingUser1 = (User)e.NewItems[0];
                    Debug.Log($"{replacedUser1.FirstName} replaced object {replacingUser1.FirstName}");
                    break;
                case NotifyCollectionChangedAction.Reset:
                    break;
                default:
                    break;
            }
        }
    } 
}