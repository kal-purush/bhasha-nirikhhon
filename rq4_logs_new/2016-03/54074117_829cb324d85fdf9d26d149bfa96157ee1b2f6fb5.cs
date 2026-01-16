using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace seaBass
{
    class Concealer
    {
        public Concealer()
        {


        }


        private void messageIn(String OrginalMessage)
        {
            
        }

        public string conceal(String message, String key)
        {
            message=message.ToLower();
            key = key.ToLower();

            string newMessage = "";

            List<List<Int32>> keyList = letterDistributionMK2(key);
            bool encoded = false;
            for (int b = 0; b < message.Count(); b++)
            {

                if (message.ElementAt(b) == 'a' && encoded == false)
                {

                    //we need to take a random one from a list, then add it, then remove it
                    Random r = new Random();
                    int index = r.Next(1, keyList.ElementAt(0).Count());
                    newMessage = newMessage + "-" + keyList.ElementAt(0).ElementAt(index) + "_";
                    keyList.ElementAt(0).RemoveAt(index);
                    encoded = true;
                }

                if (message.ElementAt(b) == 'b' && encoded == false)
                {

                    //we need to take a random one from a list, then add it, then remove it
                    Random r = new Random();
                    int index = r.Next(1, keyList.ElementAt(1).Count());
                    newMessage = newMessage + "-" + keyList.ElementAt(1).ElementAt(index) + "_";
                    keyList.ElementAt(1).RemoveAt(index);
                    encoded = true;
                }


                if (message.ElementAt(b) == 'c' && encoded == false)
                {

                    //we need to take a random one from a list, then add it, then remove it
                    //int index = keyList.ElementAt(2).Count() - 1;
                    Random r = new Random();
                    int index = r.Next(1, keyList.ElementAt(2).Count());
                    newMessage = newMessage + "-" + keyList.ElementAt(2).ElementAt(index) + "_";
                    keyList.ElementAt(2).RemoveAt(index);
                    encoded = true;
                }

                if (message.ElementAt(b) == 'd' && encoded == false)
                {

                    Random r = new Random();
                    int index = r.Next(1, keyList.ElementAt(3).Count());
                    newMessage = newMessage + "-" + keyList.ElementAt(3).ElementAt(index) + "_";
                    keyList.ElementAt(3).RemoveAt(index);
                    encoded = true;
                    //tacos for jlofdd rrr
                }

                if (message.ElementAt(b) == 'e' && encoded == false)
                {

                    Random r = new Random();
                    int index = r.Next(1, keyList.ElementAt(4).Count());
                    newMessage = newMessage + "-" + keyList.ElementAt(4).ElementAt(index) + "_";
                    keyList.ElementAt(4).RemoveAt(index);
                    encoded = true;
                }







                if (encoded == false)
                {
                    newMessage = newMessage + message.ElementAt(b);
                    encoded = true;
                }


                encoded = false;

            }

            return newMessage;
            }

            


        

        public string deConCeal(string codedMessge, string key)
        {
            string cleanedMessage = "";

            codedMessge.ToLower();
            key.ToLower();
            bool start = false;
            bool finish = false;
            string tempIndex="";
            string indexkeys = "";
            string lettersFromKey = "";
            for (int i = 0; i < codedMessge.Count(); i++)
            {
                if(start==false && finish == false)
                {
                    if (codedMessge.ElementAt(i) != '-')
                    {
                        cleanedMessage = cleanedMessage + codedMessge.ElementAt(i);
                    }
                   // cleanedMessage = cleanedMessage + codedMessge.ElementAt(i);
                }

                if (codedMessge.ElementAt(i) == '_')
                {
                    finish = true;
                }


                if (start == true && finish==false)
                {

                  tempIndex = tempIndex + codedMessge.ElementAt(i).ToString();
      
                   
                    
                }

                if(start==true && finish == true)
                {
                    indexkeys = indexkeys + tempIndex+" ";
                    start = false;
                    finish = false;
                    int index = int.Parse(tempIndex);
                    char letter= key.ElementAt(index);
                    lettersFromKey = lettersFromKey + letter;
                    cleanedMessage = cleanedMessage + letter;
                    tempIndex = "";
                }


                if (codedMessge.ElementAt(i) == '-')
                {
                    start = true;
                }

               
            }


            return cleanedMessage;
          //  return "key ids "+indexkeys+"   "+"letters ciphered "+lettersFromKey;
        }

   

        public List<List<Int32>> letterDistributionMK2(String MessageToBeDistriubted)
        {
            MessageToBeDistriubted.ToLower();
            List<Int32> Aindex = new List<Int32>();
            List<Int32> Bindex = new List<Int32>();
            List<Int32> Cindex = new List<Int32>();
            List<Int32> Dindex = new List<Int32>();
            List<Int32> Eindex = new List<Int32>();

            for (int i = 0; i < MessageToBeDistriubted.Count(); i++)
            {
                if (MessageToBeDistriubted.ElementAt(i) == 'a')
                {
                    Aindex.Add(i);
                }
                if (MessageToBeDistriubted.ElementAt(i) == 'b')
                {
                    Bindex.Add(i);
                }
                if (MessageToBeDistriubted.ElementAt(i) == 'c')
                {
                    Cindex.Add(i);
                }
                if (MessageToBeDistriubted.ElementAt(i) == 'd')
                {
                    Dindex.Add(i);
                }
                if (MessageToBeDistriubted.ElementAt(i) == 'e')
                {
                    Eindex.Add(i);
                }
            }

            List<List<Int32>> Distribution = new List<List<Int32>>();

            Distribution.Add(Aindex); //0
            Distribution.Add(Bindex); //1
            Distribution.Add(Cindex);//2
            Distribution.Add(Dindex);//3
            Distribution.Add(Eindex);//4
           

           
            return Distribution;

        }


    }


}