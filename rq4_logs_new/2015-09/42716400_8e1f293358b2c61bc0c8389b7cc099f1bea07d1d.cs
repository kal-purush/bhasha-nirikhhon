using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace XlentLock
{
    class LockService
    {
        public enum LockResponse
        {
            Locked = 0,
            Unlocked = 1,
            GuessLower = 2,
            GuessHigher = 3
        }

        private int _secretNum;
        public LockService()
        {
            var _random = new Random();
            _secretNum = _random.Next(1000);
        }

        public Enum Guess(int guess)
        {
            Enum retVal;
            if(guess == _secretNum)
            {
                retVal = LockResponse.Unlocked;
            }
            else if(guess < _secretNum)
            {
                retVal = LockResponse.GuessHigher;
            }
            else if(guess > _secretNum)
            {
                retVal = LockResponse.GuessLower;
            }
            return retVal;
        }
    }
}