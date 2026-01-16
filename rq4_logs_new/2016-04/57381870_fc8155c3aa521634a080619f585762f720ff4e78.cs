using System;
using System.Collections.Generic;

namespace Bank
{
    static class BankOperation
    {
        private static List<string> vendor = new List<string> { "American Express", "JCB", "Maestro", "MasterCard", "VISA"};
        private static int tempControlNumber = 0;

        public static string GetCreditCardVendor(string cardNumber)
        {
            int code = Convert.ToInt32(cardNumber.Substring(0, 4));

            if((code >= 3400 && code <= 3499) || (code >= 3700 && code <= 3799))
            {
                return vendor[0];
            }

            else if (code >= 3528 && code <= 3589)
            {
                return vendor[1];
            }

            else if ((code >= 5000 && code <= 5099) || (code >= 5600 && code <= 5699) || (code >= 6900 && code <= 6999))
            {
                return vendor[2];
            }

            else if ((code >= 2221 && code <= 2720) || (code >= 5100 && code <= 5599))
            {
                return vendor[3];
            }

            else if ((code >= 4000 && code <= 4999))
            {
                return vendor[4];
            }

            return "Unknown";
        }

        public static bool IsCreditCardNumberValid(string cardNumber)
        {
            string code = cardNumber.Replace(" ", string.Empty);
            List<int> oddNumbers = new List<int>();
            List<int> pairNumbers = new List<int>();
            int result = 0;

            #region PAIR CARD NUMBER LENGTH
            if (code.Length % 2 == 0)
            {

                for (int i = 0; i < code.Length; i += 2 )
                {
                    oddNumbers.Add(Convert.ToInt32(code.Substring(i, 1)));
                }

                for (int i = 1; i < code.Length; i += 2)
                {
                    pairNumbers.Add(Convert.ToInt32(code.Substring(i, 1)));
                }

                for (int i = 0; i < oddNumbers.Count; i++)
                {
                    int x = oddNumbers[i] * 2;

                    if(x > 9)
                    {
                        x = x - 9;
                    }

                    oddNumbers[i] = x;
                }

                for (int i = 0; i < oddNumbers.Count; i++)
                {
                    result += (oddNumbers[i] + pairNumbers[i]);
                }

                tempControlNumber = result % 10;

                if (result % 10 == 0)
                {
                    return true;
                }
            }
            #endregion

            #region ODD CARD NUMBER LENGTH
            else
            {
                for (int i = 1; i < code.Length; i += 2)
                {
                    pairNumbers.Add(Convert.ToInt32(code.Substring(i, 1)));
                }

                for (int i = 0; i < code.Length; i += 2)
                {
                    oddNumbers.Add(Convert.ToInt32(code.Substring(i, 1)));
                }

                for (int i = 0; i < pairNumbers.Count; i++)
                {
                    int x = pairNumbers[i] * 2;

                    if (x > 9)
                    {
                        x = x - 9;
                    }

                    pairNumbers[i] = x;
                }

                for (int i = 0; i < oddNumbers.Count; i++)
                {
                    result += oddNumbers[i];

                    if (i < pairNumbers.Count)
                    {
                        result += pairNumbers[i];
                    }
                }

                tempControlNumber = result % 10;

                if (result % 10 == 0)
                {
                    return true;
                }
            }
            #endregion

            return false;
        }

        public static string GenerateNextCreditCardNumber(string cardNumber)
        {
            Random number = new Random();
            string code = cardNumber.Replace(" ", string.Empty);
            string strfirstPart = code.Substring(0, 4);
            int intFirstPart = Convert.ToInt32(strfirstPart);
            string secondPart = null;
            int sum = (intFirstPart / 1000) + ((intFirstPart % 1000) / 100) + ((intFirstPart % 100) / 10) + intFirstPart % 10;

            for (int i = 0; i < code.Length - (strfirstPart.Length + 1); i++)
            {
                int currentRandom = number.Next(0, 9);
                secondPart += currentRandom;
                sum += currentRandom;
            }

            int controlNumber = 0;
            int part = sum % 10;

            string alphaCode = strfirstPart + secondPart + controlNumber;
            IsCreditCardNumberValid(alphaCode);

            if (tempControlNumber != 0)
            {
                controlNumber = 10 - tempControlNumber;
            }

            string newCode = strfirstPart + secondPart + controlNumber;
            return newCode;
        }
    }
}