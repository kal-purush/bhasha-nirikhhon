using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace cryptography
{
    class Trithemius : Cryptographer
    {
        string motto = "";
        List<double> coefficient = new List<double>();
        public Trithemius() { }
        public Trithemius(int Key)
        {
            this.Key = Key;
        }
        public Trithemius(int Key, string Text)
            : this(Key)
        {
            this.Text = Text;
        }

        public override string Encrypt()
        {
            string answer = "";
            int t = 0;
            foreach (char symbol in Text)
            {
                if (!UpdateKey(t++)) return "";
                answer += (char)(((int)symbol + Key + AlphabetLength) % AlphabetLength);
            }
            return answer;

        }


        public override string Decrypt()
        {
            string answer = "";
            int t = 0;
            foreach (char symbol in Text)
            {
                if (!UpdateKey(t++)) return "";
                answer += (char)(((int)symbol - Key + AlphabetLength) % AlphabetLength);
            }
            return answer;
        }
        private bool UpdateKey(int t)
        {
            long key = 0;
            if (motto != "")
                key = (int)(char)motto[t % motto.Length];
            else
            {

                for (int i = coefficient.Count - 1; i >= 0; i--)
                {
                    try
                    {
                        key += Convert.ToInt32(Math.Pow(t, i) * coefficient[i]);
                    }
                    catch(OverflowException)
                    {
                        MessageBox.Show("Cut down on the coeficients!!");
                        return false;
                    }
                }
            }
            Key = (int)key % AlphabetLength;
            return true;
        }
        public override bool SetKey(string text)
        {
            motto = "";
            coefficient.Clear();
            if (text == "") return false;
            if (text[0] == '"' && text[text.Length - 1] == '"' && text.Length > 2)
            {
                motto = text.Substring(1, text.Length - 2);
                return true;
            }
            else
            {
                int pos = 0;
                do
                {

                    for (int i = pos; i < text.Length; i++)
                    {
                        if (text[i] == ' ' || i == text.Length - 1)
                        {
                            double k;
                            if (double.TryParse(text.Substring(pos, i - pos + 1), out k) && Math.Abs(k) < AlphabetLength)
                            {
                                coefficient.Add(k);

                            }
                            else
                            {
                                return false;
                            }
                            pos = i;
                        }

                    }
                    pos++;

                }
                while (pos < text.Length);
                if (coefficient.Count > 0) return true;
                else
                {

                    double k;
                    if (double.TryParse(text.Substring(0, text.Length), out k))
                    {
                        coefficient.Add(k);
                        return true;
                    }
                    return false;

                }
            }

        }
    }
}