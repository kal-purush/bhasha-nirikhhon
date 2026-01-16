namespace Linq
{
    class Dictionary
    {
        private readonly dynamic[] _rusWords = new dynamic[5];
        private readonly dynamic[] _engWords = new dynamic[5];

        public Dictionary()
        {
            _rusWords[0] = "книга"; _engWords[0] = "book";
            _rusWords[1] = "ручка"; _engWords[1] = "pen";
            _rusWords[2] = "солнце"; _engWords[2] = "sun";
            _rusWords[3] = "яблоко"; _engWords[3] = "apple";
            _rusWords[4] = "стол"; _engWords[4] = "table";
        }

        public dynamic this[string index]
        {
            get
            {
                for (int i = 0; i < _rusWords.Length; i++)
                {
                    if (_rusWords[i] == index)
                        return _rusWords[i] + " - " + _engWords[i];
                    else if (_engWords[i] == index)
                        return _engWords[i] + " - " + _rusWords[i];
                }
                return string.Format("{0} - no translation for this word.", index);
            }
        }

        public dynamic this[int index]
        {
            get
            {
                if (index >= 0 && index < _rusWords.Length)
                    return _rusWords[index] + " - " + _engWords[index];
                else
                    return "Attempt to access outside the array.";
            }
        }
    }
}