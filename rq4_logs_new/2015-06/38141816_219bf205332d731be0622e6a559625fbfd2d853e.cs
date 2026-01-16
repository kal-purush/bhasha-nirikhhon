using System;
using System.Collections.Generic;

namespace AyD_Examen2.SpellOutCheckAmount
{
   public class SpellOutCheckAmount
    {

        public static Tuple<int, int> SplitNumberInIntegerAndDecimals(double number)
        {
            int intergers = (int)(Math.Floor(number));
            int decimals = Convert.ToInt32((number - Math.Floor(number)) * 100);
            return Tuple.Create(intergers, decimals);
        }

        public static List<int> InsertIntsInList(int number)
        {
            List<int> listOfNumbreInts = new List<int>();
            while (number > 0)
            {
                var numberToInsert = (number) % 10;
                listOfNumbreInts.Add(numberToInsert);
                number = number / 10;
            }
            return listOfNumbreInts;
        }

        public static String ConvertNumberToStringEquivalentWords(double number)
        {
            var IntergerAndDecimals = SplitNumberInIntegerAndDecimals(number);
            List<int> listOfnumersInteger = InsertIntsInList(IntergerAndDecimals.Item1);
            string decimals = IntergerAndDecimals.Item2.ToString();
            string numberInLetters = "";
            if (listOfnumersInteger.Count != 0)
            {
                int pos = 0;
                int OneDigits = 0;
                foreach (var numberInList in listOfnumersInteger)
                {
                    if (pos == 0 && listOfnumersInteger.Count == 1 || pos == 3 && listOfnumersInteger.Count == 4 ||
                        pos == 6 && listOfnumersInteger.Count == 7)
                        numberInLetters = number1DigitsToString(numberInList) + " " + UnitsNumbers(pos) + "  " + numberInLetters;
                    else if (pos == 0 || pos == 3 || pos == 6)
                        OneDigits = numberInList;
                    if (pos == 1 || pos == 4 || pos == 7)
                    {
                        if (numberInList == 1)
                            numberInLetters = number2DigitsToString(10 + OneDigits, OneDigits) + " " + UnitsNumbers(pos - 1) + " " + numberInLetters;
                        else if (numberInList >= 2)
                            numberInLetters = number2DigitsToString(numberInList, OneDigits) + " " + UnitsNumbers(pos - 1) + " " + numberInLetters;
                        else if (numberInList == 0)
                            if (OneDigits > 0)
                                numberInLetters = number1DigitsToString(OneDigits) + " " + UnitsNumbers(pos - 1) + " " + numberInLetters;
                            else if (numberInList == 0 && OneDigits == 0)
                                if (pos == 4)
                                    numberInLetters = UnitsNumbers(pos - 1) + " " + numberInLetters;
                                else if (pos == 7)
                                    numberInLetters = UnitsNumbers(6) + " " + numberInLetters;
                    }
                    if (pos == 2 || pos == 5 || pos == 8 || pos == 9)
                        if (numberInList > 0)
                            numberInLetters = number1DigitsToString(numberInList) + " " + UnitsNumbers(pos) + " " + numberInLetters;
                    pos = pos + 1;
                }
            }
            else
                numberInLetters = "CERO ";
            if (decimals == null || decimals.Equals("0"))
                decimals = "0";
            return numberInLetters + "AND " + decimals + "/100 DOLLARS";
        }

        public static string number1DigitsToString(int numberInList)
        {
            string numberStringReturn = "";
            Dictionary<int, string> DictionaryNumber = DictionaryForNumber1To9();
            numberStringReturn = searchInDictionary(DictionaryNumber, numberInList);
            return numberStringReturn;
        }
        public static string number2DigitsToString(int numberInList, int OneDigits)
        {
            string numberStringReturn = "";
            if (numberInList >= 10 && numberInList < 20)
            {
                Dictionary<int, string> DictionaryNumber = DictionaryForNumber10To19();
                numberStringReturn = searchInDictionary(DictionaryNumber, numberInList);
            }
            else if (numberInList >= 2)
            {
                Dictionary<int, string> DictionaryNumber = DictionaryForNumberForConvination();
                Dictionary<int, string> Dictionary1to9 = DictionaryForNumber1To9();
                numberStringReturn = searchInDictionary(DictionaryNumber, numberInList * 10) + "-" +
                                     searchInDictionary(Dictionary1to9, OneDigits) + " ";
            }
            return numberStringReturn;
        }

        public static string UnitsNumbers(int pos)
        {
            string numberStringReturn = "";
            Dictionary<int, string> DictionaryNumber = DictionaryForPosition();
            numberStringReturn = searchInDictionary(DictionaryNumber, pos);
            return numberStringReturn;
        }

        public static string searchInDictionary(Dictionary<int, string> Dictionary, int objectToFind)
        {
            string ObjectToReturn = "";
            foreach (var objectToSearch in Dictionary)
            {
                if (objectToFind == objectToSearch.Key)
                    ObjectToReturn = objectToSearch.Value;
            }
            return ObjectToReturn;
        }
        public static Dictionary<int, string> DictionaryForPosition()
        {
            Dictionary<int, string> DictionaryPosition = new Dictionary<int, string>();
            DictionaryPosition.Add(2, "HUNDRED"); DictionaryPosition.Add(3, "THOUSAND"); DictionaryPosition.Add(5, "HUNDRED");
            DictionaryPosition.Add(6, "MILLION"); DictionaryPosition.Add(8, "HUNDRED"); DictionaryPosition.Add(9, "BILLION");
            return DictionaryPosition;
        }
        public static Dictionary<int, string> DictionaryForNumber1To9()
        {
            Dictionary<int, string> DictionaryNumber = new Dictionary<int, string>();
            DictionaryNumber.Add(1, "ONE"); DictionaryNumber.Add(2, "TWO"); DictionaryNumber.Add(3, "THREE");
            DictionaryNumber.Add(4, "FOUR"); DictionaryNumber.Add(5, "FIVE"); DictionaryNumber.Add(6, "SIX");
            DictionaryNumber.Add(7, "SEVEN"); DictionaryNumber.Add(8, "EIGHT"); DictionaryNumber.Add(9, "NINE");
            return DictionaryNumber;
        }
        public static Dictionary<int, string> DictionaryForNumber10To19()
        {
            Dictionary<int, string> DictionaryNumber = new Dictionary<int, string>();
            DictionaryNumber.Add(10, "TEN"); DictionaryNumber.Add(11, "ELEVEN"); DictionaryNumber.Add(12, "TWELVE");
            DictionaryNumber.Add(13, "THIRTEEN"); DictionaryNumber.Add(14, "FOURTEEN"); DictionaryNumber.Add(15, "FIFTEEN");
            DictionaryNumber.Add(16, "SIXTEEN"); DictionaryNumber.Add(17, "SEVENTEEN"); DictionaryNumber.Add(18, "EIGHTEEN");
            DictionaryNumber.Add(19, "NINETEEN");
            return DictionaryNumber;
        }
        public static Dictionary<int, string> DictionaryForNumberForConvination()
        {
            Dictionary<int, string> DictionaryNumber = new Dictionary<int, string>();
            DictionaryNumber.Add(20, "TWENTY"); DictionaryNumber.Add(30, "THIRTY "); DictionaryNumber.Add(40, "FORTY");
            DictionaryNumber.Add(50, "FIFTY"); DictionaryNumber.Add(60, "SIXTY"); DictionaryNumber.Add(70, "SEVENTY");
            DictionaryNumber.Add(80, "EIGHTY"); DictionaryNumber.Add(90, "NINETY");
            return DictionaryNumber;
        }
    }
}