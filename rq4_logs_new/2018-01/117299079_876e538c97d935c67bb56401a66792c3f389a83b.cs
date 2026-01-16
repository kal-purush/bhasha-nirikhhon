using System;

namespace Things.Favorites
{
    class Candy
    {
        protected string _type;
        protected bool _isDelicious;

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

        public bool IsDelicious
        {
            get
            {
                return _isDelicious;
            }
            set
            {
                if (value)
                {
                    _isDelicious = true;
                }
                if (!value)
                {
                    _isDelicious = false;
                }
            }
        }
        public Candy()
        {
            Type = "SourPatchKids";
        }
        public string Enjoying(bool IsDelicious) {
            {
                return ("Sour candy is the best");
            }
       }
    }
}


