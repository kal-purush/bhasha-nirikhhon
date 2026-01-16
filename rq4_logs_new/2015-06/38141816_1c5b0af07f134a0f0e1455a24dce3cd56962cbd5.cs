using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace AyD_Examen2.SpellOutCheckAmount
{
   public class CheckAmount
    {
       double Amount { get; set; }
       public CheckAmount(double amount)
       {
           Amount = amount;
       }

       public override string ToString()
       {
           var intergerAndDecimals = SplitNumberInIntegerAndDecimals(Amount);
           var listOfnumersInteger = InsertIntsInList(intergerAndDecimals.Item1);
           var decimals = intergerAndDecimals.Item2.ToString(CultureInfo.InvariantCulture);
           var numberInLetters = "";
           if (listOfnumersInteger.Count != 0)
           {
               var pos = 0;
               var oneDigits = 0;
               foreach (var numberInList in listOfnumersInteger)
               {
                   if (pos == 0 && listOfnumersInteger.Count == 1 || pos == 3 && listOfnumersInteger.Count == 4 ||
                       pos == 6 && listOfnumersInteger.Count == 7)
                       numberInLetters = Number1DigitsToString(numberInList) + " " + UnitsNumbers(pos) + "  " + numberInLetters;
                   else if (pos == 0 || pos == 3 || pos == 6)
                       oneDigits = numberInList;
                   if (pos == 1 || pos == 4 || pos == 7)
                   {
                       if (numberInList == 1)
                           numberInLetters = Number2DigitsToString(10 + oneDigits, oneDigits) + " " + UnitsNumbers(pos - 1) + " " + numberInLetters;
                       else if (numberInList >= 2)
                           numberInLetters = Number2DigitsToString(numberInList, oneDigits) + " " + UnitsNumbers(pos - 1) + " " + numberInLetters;
                       else if (numberInList == 0)
                           if (oneDigits > 0)
                               numberInLetters = Number1DigitsToString(oneDigits) + " " + UnitsNumbers(pos - 1) + " " + numberInLetters;
                           else if (numberInList == 0 && oneDigits == 0)
                               switch (pos)
                               {
                                   case 4:
                                       numberInLetters = UnitsNumbers(pos - 1) + " " + numberInLetters;
                                       break;
                                   case 7:
                                       numberInLetters = UnitsNumbers(6) + " " + numberInLetters;
                                       break;
                               }
                   }
                   if (pos == 2 || pos == 5 || pos == 8 || pos == 9)
                       if (numberInList > 0)
                           numberInLetters = Number1DigitsToString(numberInList) + " " + UnitsNumbers(pos) + " " + numberInLetters;
                   pos = pos + 1;
               }
           }
           else
               numberInLetters = "ZERO ";
           if (decimals.Equals("0"))
               decimals = "0";
           return (numberInLetters + (decimals.Equals("0") ? "" : "AND " + decimals + "/100 DOLLARS")).Trim();
       }

       public static Tuple<int, int> SplitNumberInIntegerAndDecimals(double number)
        {
            var intergers = (int)(Math.Floor(number));
            var decimals = Convert.ToInt32((number - Math.Floor(number)) * 100);
            return Tuple.Create(intergers, decimals);
        }

        public static List<int> InsertIntsInList(int number)
        {
            var listOfNumbreInts = new List<int>();
            while (number > 0)
            {
                var numberToInsert = (number) % 10;
                listOfNumbreInts.Add(numberToInsert);
                number = number / 10;
            }
            return listOfNumbreInts;
        }

        public static string Number1DigitsToString(int numberInList)
        {
            var dictionaryNumber = DictionaryForNumber1To9();
            var numberStringReturn = SearchInDictionary(dictionaryNumber, numberInList);
            return numberStringReturn;
        }
        public static string Number2DigitsToString(int numberInList, int oneDigits)
        {
            var numberStringReturn = "";
            if (numberInList >= 10 && numberInList < 20)
            {
                var dictionaryNumber = DictionaryForNumber10To19();
                numberStringReturn = SearchInDictionary(dictionaryNumber, numberInList);
            }
            else if (numberInList >= 2)
            {
                var dictionaryNumber = DictionaryForNumberForConvination();
                var dictionary1To9 = DictionaryForNumber1To9();
                numberStringReturn = SearchInDictionary(dictionaryNumber, numberInList * 10) + "-" +
                                     SearchInDictionary(dictionary1To9, oneDigits) + " ";
            }
            return numberStringReturn;
        }

        public static string UnitsNumbers(int pos)
        {
            var dictionaryNumber = DictionaryForPosition();
            var numberStringReturn = SearchInDictionary(dictionaryNumber, pos);
            return numberStringReturn;
        }

        public static string SearchInDictionary(Dictionary<int, string> dictionary, int objectToFind)
        {
            var objectToReturn = "";
            foreach (var objectToSearch in dictionary.Where(objectToSearch => objectToFind == objectToSearch.Key))
            {
                objectToReturn = objectToSearch.Value;
            }
            return objectToReturn;
        }
        public static Dictionary<int, string> DictionaryForPosition()
        {
            var dictionaryPosition = new Dictionary<int, string>
            {
                {2, "HUNDRED"},
                {3, "THOUSAND"},
                {5, "HUNDRED"},
                {6, "MILLION"},
                {8, "HUNDRED"},
                {9, "BILLION"}
            };
            return dictionaryPosition;
        }
        public static Dictionary<int, string> DictionaryForNumber1To9()
        {
            var dictionaryNumber = new Dictionary<int, string>
            {
                {1, "ONE"},
                {2, "TWO"},
                {3, "THREE"},
                {4, "FOUR"},
                {5, "FIVE"},
                {6, "SIX"},
                {7, "SEVEN"},
                {8, "EIGHT"},
                {9, "NINE"}
            };
            return dictionaryNumber;
        }
        public static Dictionary<int, string> DictionaryForNumber10To19()
        {
            var dictionaryNumber = new Dictionary<int, string>
            {
                {10, "TEN"},
                {11, "ELEVEN"},
                {12, "TWELVE"},
                {13, "THIRTEEN"},
                {14, "FOURTEEN"},
                {15, "FIFTEEN"},
                {16, "SIXTEEN"},
                {17, "SEVENTEEN"},
                {18, "EIGHTEEN"},
                {19, "NINETEEN"}
            };
            return dictionaryNumber;
        }
        public static Dictionary<int, string> DictionaryForNumberForConvination()
        {
            var dictionaryNumber = new Dictionary<int, string>
            {
                {20, "TWENTY"},
                {30, "THIRTY "},
                {40, "FORTY"},
                {50, "FIFTY"},
                {60, "SIXTY"},
                {70, "SEVENTY"},
                {80, "EIGHTY"},
                {90, "NINETY"}
            };
            return dictionaryNumber;
        }
    }
}