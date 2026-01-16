using System;

namespace Client
{
    class ClientFrameManager
    {
        public string ConnectionBuild(string login, string password)
        {
            return "LOGIN;" + login + ";" + password;
        }

        public string GetBalanceBuild()
        {
            return "GBAL;";
        }

        public string UpdatebalanceBuild(int amount)
        {
            return "UBAL;" + amount.ToString();
        }

        public string GetFrameHeader(string frame)
        {
            string[] parameters;
            string[] stringSeparators = new string[] { ";" };

            parameters = frame.Split(stringSeparators, StringSplitOptions.None);

            return parameters[0];
        }

        public string ACKConnectionRead(string frame)
        {
            string[] parameters;
            string[] stringSeparators = new string[] { ";" };

            parameters = frame.Split(stringSeparators, StringSplitOptions.None);
            return parameters[1];
        }

        public int SendBalanceRead(string frame)
        {
            string[] parameters;
            string[] stringSeparators = new string[] { ";" };

            parameters = frame.Split(stringSeparators, StringSplitOptions.None);
            return Int32.Parse(parameters[1]);
        }

        public bool ACKUpdateBalanceBuild(string frame)
        {
            string[] parameters;
            string[] stringSeparators = new string[] { ";" };

            parameters = frame.Split(stringSeparators, StringSplitOptions.None);
            if (parameters[1].Equals("True"))
                return true;
            else
                return false;
        }
    }
}