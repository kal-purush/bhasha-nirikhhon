using System;
using System.Collections.Generic;
using TennisClub.AppCore.model.impl;

namespace TennisClub.WpfDesktop
{
    

class ChildMock
    {
        public IEnumerable<Child> Children()
        {
            List<Child> children = new List<Child>
            {
                new Child( "John", "Tennis", GameLevel.Average, DayOfWeek.Monday, new DateTime(2010, 12, 1)),
                new Child("Alice", "Tennis", GameLevel.Average, DayOfWeek.Monday, new DateTime(2010, 12, 1)),
                new Child( "Ivan", "Tennis", GameLevel.Average, DayOfWeek.Monday, new DateTime(2010, 12, 1)),
                new Child( "Bon", "Tennis", GameLevel.Average, DayOfWeek.Monday, new DateTime(2010, 12, 1)),
                new Child( "Claire", "Tennis", GameLevel.Average, DayOfWeek.Monday, new DateTime(2010, 12, 1)),
                new Child( "Mary", "Tennis", GameLevel.Average, DayOfWeek.Monday, new DateTime(2010, 12, 1)),
                // day of week test
                new Child("Vova", "Tennis", GameLevel.Average, DayOfWeek.Tuesday, new DateTime(2010, 12, 1)),
                new Child( "Petya", "Tennis", GameLevel.Average, DayOfWeek.Monday, new DateTime(2010, 12, 1)),
                // age test
                new Child("Vasya", "Tennis", GameLevel.Average, DayOfWeek.Sunday, new DateTime(2010, 12, 1)),
                new Child("Jora", "Tennis", GameLevel.Average, DayOfWeek.Sunday, new DateTime(2012, 12, 1)),
                new Child( "Helen", "Tennis", GameLevel.Average, DayOfWeek.Sunday, new DateTime(2013, 12, 1)),
                new Child( "Joseline", "Tennis", GameLevel.Average, DayOfWeek.Sunday, new DateTime(2015, 12, 1)),
                // preferences test
                new Child( "Jolene", "Tennis", GameLevel.Beginner, DayOfWeek.Monday, new DateTime(2010, 12, 1)),
                new Child( "Timur", "Tennis", GameLevel.Average, DayOfWeek.Monday, new DateTime(2010, 12, 1)),
                new Child( "Nadya", "Tennis", GameLevel.Beginner, DayOfWeek.Monday, new DateTime(2010, 12, 1)),
                // age
                new Child( "Gena", "Tennis", GameLevel.Professional, DayOfWeek.Tuesday, new DateTime(1998, 12, 1))
            };
            return children;
        }
    }
}