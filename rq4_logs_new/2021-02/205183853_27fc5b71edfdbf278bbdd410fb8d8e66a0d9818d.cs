using System.Collections.Generic;
namespace Linear_DataStructures{
    public class FirstRepeatCharacter
    {
        public void FirstNonRepeatChr(string s)
        {
            Dictionary<char,int> listCh= new System.Collections.Generic.Dictionary<char,int>();
            foreach(char c in s)
            {
                if(listCh.ContainsKey(c))
                {
                    listCh[c]+=1;
                }
                else
                {
                    listCh.Add(c,1);
                }
                
            }
            foreach(KeyValuePair<char,int> kv in listCh)
            {
                if(kv.Value==1)
                    System.Console.WriteLine(kv.Key + ":" +kv.Value);
                
            }

        }
          public char FirstRepeatChr(string s)
        {
            HashSet<char> listCh= new System.Collections.Generic.HashSet<char>();
            foreach (char c in s)
            {
                if (listCh.Contains(c))
                {
                    System.Console.WriteLine(c);
                    return c;
                }
                listCh.Add(c);
                
            }
            return char.MinValue;
        }

        public static void  DriverCode()

        {
            string s = "geek of geek";
            FirstRepeatCharacter first= new FirstRepeatCharacter();
            s="abc";
            System.Console.WriteLine(first.FirstRepeatChr(s));
            first.FirstNonRepeatChr(s);

        }
    }
}