using System;

namespace GameCore
{
    class Player
    {
        public string name = "";
        public bool isFirstPlayer = false;
        public enum Avatar {
            malePirate,
            femalePirate,
            maleNavy,
            femaleNavy,
        }
    }
}