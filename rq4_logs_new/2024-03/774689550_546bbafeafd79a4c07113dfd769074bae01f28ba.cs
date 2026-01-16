using DataAccess.CRUD;
using DTO;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CoreApp
{
    public class SecretaryManager
    {
        public void Create(Secretary secretary)
        {
            var sc = new SecretaryCrudFactory();


            if (!IsValidName(secretary.Name))
            {
                throw new Exception("Invalid name format");
            }
            else if (!IsValidLastName(secretary.LastName))
            {
                throw new Exception("Invalid Lastname format");
            }
            else if (!IsValidPhoneNumber(secretary.PhoneNumber))
            {
                throw new Exception("Invalid phone number format");
            }
            else if (!IsValidEmail(secretary.Email))
            {
                throw new Exception("Email is required");
            }
            else if (!IsValidPassword(secretary.Password))
            {
                throw new Exception("Invalid Password format");
            }
            else if (!IsValidSex(secretary.Sex))
            {
                throw new Exception("Invalid Sex format");
            }
            else if (!IsValidBirthDate(secretary.BirthDate))
            {
                throw new Exception("Invalid birth date format");
            }
            else if (!IsValidRole(secretary.Role))
            {
                throw new Exception("Invalid role format");
            }
            else if (!IsValidStatus(secretary.Status))
            {
                throw new Exception("Invalid status value");
            }
            else if (!IsValidAdress(secretary.Address))
            {
                throw new Exception("Invalid Adress format");
            }
            sc.Create(secretary);
        }

        public List<T> RetrieveAll<T>()
        {
            var sc = new SecretaryCrudFactory();
            return sc.RetrieveAll<T>();
        }

        public Secretary RetrieveById(int secretaryID)
        {
            var sc = new SecretaryCrudFactory();
            return sc.RetrieveById<Secretary>(secretaryID);
        }


        public void Update(Secretary secretary)
        {
            var sc = new SecretaryCrudFactory();

            if (!IsValidName(secretary.Name))
            {
                throw new Exception("Invalid name format");
            }
            else if (!IsValidLastName(secretary.LastName))
            {
                throw new Exception("Invalid Lastname format");
            }
            else if (!IsValidPhoneNumber(secretary.PhoneNumber))
            {
                throw new Exception("Invalid phone number format");
            }
            else if (!IsValidEmail(secretary.Email))
            {
                throw new Exception("Email is required");
            }
            else if (!IsValidPassword(secretary.Password))
            {
                throw new Exception("Invalid Password format");
            }
            else if (!IsValidSex(secretary.Sex))
            {
                throw new Exception("Invalid Sex format");
            }
            else if (!IsValidBirthDate(secretary.BirthDate))
            {
                throw new Exception("Invalid birth date format");
            }
            else if (!IsValidRole(secretary.Role))
            {
                throw new Exception("Invalid role format");
            }
            else if (!IsValidStatus(secretary.Status))
            {
                throw new Exception("Invalid status value");
            }
            else if (!IsValidAdress(secretary.Address))
            {
                throw new Exception("Invalid Adress format");
            }
            else if (!IsValidID(secretary.BranchID))
            {
                throw new Exception("Invalid Branch ID");
            }
            sc.Update(secretary);

        }

        public void Delete(Secretary secretary)
        {
            var sc = new SecretaryCrudFactory();
            sc.Delete(secretary);
        }

        private bool IsValidName(string name)
        {
            return !string.IsNullOrWhiteSpace(name) && char.IsUpper(name[0]) && name.All(c => char.IsLetter(c) && !char.IsWhiteSpace(c));
        }

        private bool IsValidLastName(string name)
        {
            return !string.IsNullOrWhiteSpace(name) && char.IsUpper(name[0]) && name.All(c => char.IsLetter(c) && !char.IsWhiteSpace(c));
        }

        private bool IsValidPassword(string password)
        {
            if (string.IsNullOrWhiteSpace(password))
                return false;

            if (password.Length < 8)
                return false;

            bool hasNumber = false;
            bool hasSpecialCharacter = false;

            foreach (char c in password)
            {
                if (char.IsDigit(c))
                {
                    hasNumber = true;
                }
                else if (!char.IsLetterOrDigit(c))
                {
                    hasSpecialCharacter = true;
                }
            }

            return hasNumber && hasSpecialCharacter;
        }

        private bool IsValidSex(string sex)
        {
            if (string.IsNullOrWhiteSpace(sex))
                return false;

            string sexLower = sex.ToLower();

            return sexLower == "m" || sexLower == "f" || sexLower == "masculino" || sexLower == "femenino";
        }

        private bool IsValidEmail(string email)
        {

            string emailRegexPattern = @"^[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$";
            return Regex.IsMatch(email, emailRegexPattern);
        }

        private bool IsValidStatus(string status)
        {
            return !string.IsNullOrWhiteSpace(status) && char.IsUpper(status[0]) && status.All(c => char.IsLetter(c) || char.IsWhiteSpace(c));
        }

        private bool IsValidPhoneNumber(int phoneNumber)
        {

            return phoneNumber.ToString().Length == 8 && !phoneNumber.ToString().Contains(" ");
        }

        private bool IsValidRole(string role)
        {
            return !string.IsNullOrWhiteSpace(role) && char.IsUpper(role[0]) && role.All(c => char.IsLetter(c) && !char.IsWhiteSpace(c));
        }

        private bool IsValidAdress(string adress)
        {
            return !string.IsNullOrWhiteSpace(adress) && char.IsUpper(adress[0]) && adress.All(c => char.IsLetterOrDigit(c) || char.IsWhiteSpace(c));
        }


        private bool IsValidBirthDate(DateTime birthDate)
        {
            string expectedFormat = "yyyy-MM-dd";

            return DateTime.TryParseExact(birthDate.ToString(expectedFormat), expectedFormat, null, DateTimeStyles.None, out _);
        }

        private bool IsValidID(int id)
        {
            return id > 0;
        }

    }
}