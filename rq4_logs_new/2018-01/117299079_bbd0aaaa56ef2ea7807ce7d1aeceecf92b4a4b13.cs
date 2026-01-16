using System;


namespace Things.Favorites
{
    class Meme
    {
        protected string _type;
        protected bool _isImage;
        protected string _origin;
        protected int _phrase;

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

        public bool IsImage
        {
            get
            {
                return _isImage;
            }
            set
            {
                if (value)
                {
                    _isImage = true;
                }
                if (!value)
                {
                    _isImage = false;
                }
            }
        }

        public string Origin
        {
            get
            {
                return _origin;
            }
            set
            {
                _origin = value;
            }
        }

        public int Phrase
        {
            get
            {
                return _phrase;
            }
            set
            {
                _phrase = 1;
            }
        }
        public Meme()
        {
            _type = "Doggo";
            _origin = "Instagram";
        }
        public bool MemeOrigin(bool isImage)
        {
            Console.WriteLine($"There are some pretty funny {_type} memes online.");
            Console.ReadLine();
            if (isImage)
            {
                _isImage = true;
                return true;
            }
                _isImage = false;
                return false;
        }
    }
}
