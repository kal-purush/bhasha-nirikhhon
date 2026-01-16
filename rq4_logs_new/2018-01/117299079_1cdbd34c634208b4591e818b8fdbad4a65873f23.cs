using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Things.Favorites
{
    class Plant
    {
        private string _type;
        protected bool _isIndoor;
        protected int _amountToWater;

        public string Type
        {
            get
            {
                return _type;
            }
            set
            {
                _type = value;
            }
        }
        
        public bool IsIndoor
        {
            get
            {
                return _isIndoor;
            }
            set
            {
                if (value)
                {
                    _isIndoor = true;
                }
                if (!value)
                {
                    _isIndoor = false;
                }
            }
        }

        public int AmountToWater
        {
            get
            {
                return _amountToWater;
            }
            set
            {
                _amountToWater = value;
            }
        }
        public string PlantCare(string Type, int AmountToWater)
        {
            return $"{Type} is an easy plant to take care of. Water it {AmountToWater} times per week.";
        }
    }
}