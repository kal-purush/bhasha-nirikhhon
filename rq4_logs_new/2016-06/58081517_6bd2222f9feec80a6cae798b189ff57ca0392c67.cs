using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ColdPlayProject.abstractConcept
{
    public abstract class Animal
    {
        protected string _myName;
        private int _myAge;
        private bool _hasStartedClass;
        protected double _mySpeed;
        private long _myPhoneNumber;

        protected Animal(string myName, int myAge, bool hasStartedClass, double mySpeed, long myPhoneNumber)
        {
            this._myName = myName;
            this._myAge = myAge;
            this._hasStartedClass = hasStartedClass;
            this._mySpeed = mySpeed;
            this._myPhoneNumber = myPhoneNumber;
        }

        public void Run()
        {
            Console.WriteLine("The animal can run at {0}", _mySpeed);
        }

        public abstract string ShowAnimalName();

        public virtual void CalculateSpeed()
        {
            double speed = _mySpeed*.05;
            Console.WriteLine("speed from the parent class is {0}", speed);
        }

    }
}