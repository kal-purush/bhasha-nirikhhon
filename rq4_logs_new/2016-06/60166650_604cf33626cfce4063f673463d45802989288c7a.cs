using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FrmEdit
{
    public class Customer
    {
        #region Membervariables
        private int customerNumber;
        private string firstName;
        private string lastName;
        private string eMailAdress;
        private double balancing;
        private DateTime dateLastChange;
        #endregion

        #region Constructor
        public Customer(string firstName,
                        string lastName,
                        string eMailAdress,
                        int customerNumber)
        {
            this.CustomerNumber = customerNumber;
            this.FirstName = firstName;
            this.LastName = lastName;
            this.EMailAdress = eMailAdress;
            this.Balancing = 0.0;
            this.DateLastChange = DateTime.Now;
        }
        #endregion

        #region Properties
        public int CustomerNumber
        {
            get
            {
                return (this.customerNumber);
            }
            private set
            {
                this.customerNumber = value;
                this.DateLastChange = DateTime.Now;
            }
        }
        public string FirstName
        {
            get
            {
                return (this.firstName);
            }
            set
            {
                if (value == String.Empty)
                {
                    throw new ArgumentException("FirstName does not allow empty string");
                }
                else
                {
                    this.firstName = value;
                    this.DateLastChange = DateTime.Now;
                }
            }
        }
        public string LastName
        {
            get
            {
                return (this.lastName);
            }
            set
            {
                if (value == String.Empty)
                {
                    throw new ArgumentException("LastName does not allow empty string");
                }
                else
                {
                    this.lastName = value;
                    this.DateLastChange = DateTime.Now;
                }
            }
        }
        public string EMailAdress
        {
            get
            {
                return (this.eMailAdress);
            }
            set
            {
                if (value == String.Empty)
                {
                    throw new ArgumentException("EMailAdress does not allow empty string");
                }
                else
                {
                    this.eMailAdress = value;
                    this.DateLastChange = DateTime.Now;
                }
            }
        }
        public double Balancing
        {
            get
            {
                return (this.balancing);
            }
            set
            {
                this.balancing = value;
                this.DateLastChange = DateTime.Now;
            }
        }
        public DateTime DateLastChange
        {
            get
            {
                return (this.dateLastChange);
            }
            set
            {
                if (DateTime.Compare(value, this.dateLastChange) >= 0)
                {
                    this.dateLastChange = value;
                }
                else
                {
                    throw new ArgumentException("Date is older than date of actual last change");
                }
            }
        }
        #endregion

        #region static Methods
        public static int ValidateEMailAdress(List<Customer> customerList, string eMailAdress)
        {
            int errorCode = 0;

            if (EMailAdressContainsExactlyOneAt(eMailAdress))
            {
                if (ContainsDotAfterAt(eMailAdress))
                {
                    if (LengthOfFinalPart(eMailAdress) >= 2
                        && LengthOfFinalPart(eMailAdress) <= 4)
                    {
                        if (FinalPartsContainsOnlyLetters(eMailAdress))
                        {
                            if (ContainsCharacterBeforeAt(eMailAdress))
                            {
                                if (NoDotsAtInvalidPosition(eMailAdress))
                                {
                                    if (NoInvalidSymbols(eMailAdress))
                                    {
                                        //Adress is valid
                                        if (EMailAdressNotListedYet(customerList, eMailAdress))
                                        {
                                            //Adress is new in List
                                            errorCode = 0;
                                        }
                                        else
                                        {
                                            //Adress is Listed
                                            errorCode = -8;
                                        }
                                    }
                                    else
                                    {
                                        errorCode = -7;
                                    }
                                }
                                else
                                {
                                    errorCode = -6;
                                }
                            }
                            else
                            {
                                errorCode = -5;
                            }
                        }
                        else 
                        {
                            errorCode = -4;
                        }
                    }
                    else 
                    {
                        errorCode = -3;
                    }
                }
                else
                {
                    errorCode = -2;
                }
            }
            else
            {
                errorCode = -1;
            }


            return (errorCode);
        }
        //Checks if eMailAdress contains exactly one @
        private static bool EMailAdressContainsExactlyOneAt(string eMailAdress)
        {
            bool result = true;

            if (eMailAdress.IndexOf('@') < 0)
            {
                result = false;
            }
            else
            {
                if (eMailAdress.IndexOf('@') != eMailAdress.LastIndexOf('@'))
                {
                    result = false;
                }
            }

            return (result);
        }

        //Checks if there is a '.' in the part after the @
        private static bool ContainsDotAfterAt(string eMailAdress)
        {
            bool result = true;

            if (eMailAdress.IndexOf('@') > eMailAdress.LastIndexOf('.'))
            {
                result = false;
            }

            return (result);
        }

        //Returns the number of characters after the last '.'
        private static int LengthOfFinalPart(string eMailAdress)
        {
            int lastindex = (eMailAdress.LastIndexOf('.') + 1);
            return (eMailAdress.Length - (lastindex));
        }

        //Checks if there are only Letters in the Part after the last '.'
        private static bool FinalPartsContainsOnlyLetters(string eMailAdress)
        {
            bool result = true;
            char[] temp = eMailAdress.ToCharArray(eMailAdress.LastIndexOf('.')+1,
                                                   LengthOfFinalPart(eMailAdress));

            for (int i = 0; i < temp.Length; i++)
            {
                if (!char.IsLetter(temp[i]))
                {
                    result = false;
                }
            }

            return (result);
        }

        //Checks if there is a charakter before the @
        private static bool ContainsCharacterBeforeAt(string eMailAdress)
        {
            bool result = false;

            if (eMailAdress.IndexOf('@') > 0)
            {
                char[] temp = eMailAdress.ToCharArray(0,
                                       eMailAdress.IndexOf('@'));

                for (int i = 0; i < temp.Length; i++)
                {
                    if (char.IsLetter(temp[i]))
                    {
                        result = true;
                    }
                }
            }

            return (result);
        }

        //Check if there is no '.' at the start, end of next to the @
        private static bool NoDotsAtInvalidPosition(string eMailAdress)
        {
            bool result = true;

            if (eMailAdress.IndexOf('.') == 0)
            {
                result = false;
            }
            else
            {
                if (eMailAdress.LastIndexOf('.') == eMailAdress.Length - 1)
                {
                    result = false;
                }
                else
                {
                    string[] temp = eMailAdress.Split('@');
                    if (temp[0].LastIndexOf('.') == temp[0].Length-1
                        ||
                        temp[1].IndexOf('.') == 0)
                    {
                        result = false;
                    }
                }
            }

            return (result);
        }

        //Checks if there are no invalid symbols
        private static bool NoInvalidSymbols(string eMailAdress)
        {
            bool result = true;
            string[] validSpecialSymbols = new string[]{".","!","#","$",
                                                        "%","&","'","*",
                                                        "+","-","/","=",
                                                        "?","^"," ","`",
                                                        "{","|","}","~","_","@"};
            char[] temp = eMailAdress.ToCharArray();

            for (int i = 0; i < temp.Length; i++)
            {
                if (!char.IsLetterOrDigit(temp[i])
                    && !validSpecialSymbols.Contains(temp[i].ToString()))
                {
                    result = false;
                }
            }

            return (result);
        }

        //Checks if the E-Mail-Adress is already listed in the Customerlist
        private static bool EMailAdressNotListedYet(List<Customer> customerList, string eMailAdress)
        {
            bool result = true;

            foreach (Customer customer in customerList)
            {
                if (customer.EMailAdress == eMailAdress)
                {
                    result = false;
                }
            }

            return (result);
        }
        #endregion

        #region dynamic Methods
        public override string ToString()
        {
            string result =
                this.CustomerNumber.ToString() + ";" 
                + this.FirstName + ";"
                + this.LastName + ";" 
                + this.EMailAdress + ";" 
                + this.Balancing.ToString() + ";" 
                + this.DateLastChange.ToString();

            return (result);
        }
        #endregion
    }
}