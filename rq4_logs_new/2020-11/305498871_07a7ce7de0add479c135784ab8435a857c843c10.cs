namespace AdditionalTask
{
    public class Dictionary
    {
        private string[] key = new string[5];
        private string[] value1 = new string[5];
        private string[] value2 = new string[5];

        public Dictionary()
        {
            key[0] = "книга"; value1[0] = "book"; value2[0] = "книга";
            key[1] = "ручка"; value1[1] = "pencil"; value2[1] = "олівець";
            key[2] = "солнце"; value1[2] = "sun"; value2[2] = "сонце";
            key[3] = "яблоко"; value1[3] = "apple"; value2[3] = "яблуко";
            key[4] = "стол"; value1[4] = "table"; value2[4] = "стіл";
        }

        public string this[string index]
        {
            get
            {

                for (int i = 0; i < key.Length; i++)
                {
                    if (key[i] == index)
                    {

                        return key[i] + " - " + value2[i] + " - " + value1[i];
                    }
                    else if (value1[i] == index)
                    {
                        return value1[i] + " - " + key[i] + " - " + value2[i];
                    }
                    else if (value2[i] == index)
                    {
                        return value2[i] + " - " + key[i] + " - " + value1[i];
                    }
                }

                return string.Format("{0} - нет перевода для этого слова.", index);
            }
        }

        public string this[int index]
        {
            get
            {
                if (index >= 0 && index < key.Length)
                    return key[index] + " - " + value1[index] + " - " + value2[index];
                else
                    return "Попытка обращения за пределы массива.";
            }
        }
    }
}