using System;

namespace DragRace
{
    public class Car : ICar, INitroCar
    {
        protected int _currentSpeed = 0;
        protected string _soundOfEngine = "Brrrrr...";
        protected string _name = "";

        public string GetName()
        {
            return this._name;
        }

        public virtual void UseNitrousOxideEngine()
        {
        }

        public void SpeedUp()
        {
            _currentSpeed++;
        }

        public void SlowDown()
        {
            if (_currentSpeed > 0) 
            {
                _currentSpeed--;
            }
        }

        public string ShowCurrentSpeed()
        {
            return _currentSpeed.ToString();
        }

        public void StartEngine()
        {
            Console.WriteLine(_soundOfEngine);
        }

        public int GetSpeed()
        {
            return _currentSpeed;
        }
    }
}