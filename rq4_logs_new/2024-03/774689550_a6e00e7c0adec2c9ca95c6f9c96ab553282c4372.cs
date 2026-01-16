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
	public class NurseManager
	{
		//Metodo para create


		public void Create(Nurse nurse)
		{
			var uc = new NurseCrudFactory();

			//Valiacion de forma

			if (!IsValidName(nurse.Name))
			{
				throw new Exception("Invalid name format");
			}
			else if (!IsValidLastName(nurse.LastName))
			{
				throw new Exception("Invalid Lastname format");
			}
			else if (!IsValidPhoneNumber(nurse.PhoneNumber))
			{
				throw new Exception("Invalid phone number format");
			}
			else if (!IsValidEmail(nurse.Email))
			{
				throw new Exception("Email is required");
			}
			else if (!IsValidPassword(nurse.Password))
			{
				throw new Exception("Invalid Password format");
			}
			else if (!IsValidSex(nurse.Sex))
			{
				throw new Exception("Invalid Sex format");
			}
			else if (!IsValidBirthDate(nurse.BirthDate))
			{
				throw new Exception("Invalid birth date format");
			}
			else if (!IsValidRole(nurse.Role))
			{
				throw new Exception("Invalid role format");
			}
			else if (!IsValidStatus(nurse.Status))
			{
				throw new Exception("Invalid status value");
			}
			else if (!IsValidAdress(nurse.Address))
			{
				throw new Exception("Invalid Adress format");
			}
			uc.Create(nurse);






		}

		public List<Nurse> RetrieveAll()
		{
			var uc = new NurseCrudFactory();
			return uc.RetrieveAll<Nurse>();
		}

		public Nurse RetrieveById(int nurseId)
		{
			var uc = new NurseCrudFactory();
			return uc.RetrieveById<Nurse>(nurseId);
		}


		public void Update(Nurse nurse)
		{
			var uc = new NurseCrudFactory();

			if (!IsValidName(nurse.Name))
			{
				throw new Exception("Invalid name format");
			}
			else if (!IsValidLastName(nurse.LastName))
			{
				throw new Exception("Invalid Lastname format");
			}
			else if (!IsValidPhoneNumber(nurse.PhoneNumber))
			{
				throw new Exception("Invalid phone number format");
			}
			else if (!IsValidEmail(nurse.Email))
			{
				throw new Exception("Email is required");
			}
			else if (!IsValidPassword(nurse.Password))
			{
				throw new Exception("Invalid Password format");
			}
			else if (!IsValidSex(nurse.Sex))
			{
				throw new Exception("Invalid Sex format");
			}
			else if (!IsValidBirthDate(nurse.BirthDate))
			{
				throw new Exception("Invalid birth date format");
			}
			else if (!IsValidRole(nurse.Role))
			{
				throw new Exception("Invalid role format");
			}
			else if (!IsValidStatus(nurse.Status))
			{
				throw new Exception("Invalid status value");
			}
			else if (!IsValidAdress(nurse.Address))
			{
				throw new Exception("Invalid Adress format");
			}
			uc.Update(nurse);

		}

		public void Delete(Nurse nurse)
		{
			var uc = new NurseCrudFactory();
			uc.Delete(nurse);
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
			// Verifica si la contraseña no es nula o compuesta solamente de espacios en blanco
			if (string.IsNullOrWhiteSpace(password))
				return false;

			// Verifica si la contraseña tiene al menos 8 caracteres
			if (password.Length < 8)
				return false;

			bool hasNumber = false;
			bool hasSpecialCharacter = false;

			foreach (char c in password)
			{
				// Verifica si el carácter actual es un número
				if (char.IsDigit(c))
				{
					hasNumber = true;
				}
				// Verifica si el carácter actual es un carácter especial
				else if (!char.IsLetterOrDigit(c))
				{
					hasSpecialCharacter = true;
				}
			}

			// Retorna verdadero si la contraseña tiene al menos un número y un carácter especial
			return hasNumber && hasSpecialCharacter;
		}

		private bool IsValidSex(string sex)
		{
			// Verifica si el sexo no es nulo o compuesto solamente de espacios en blanco
			if (string.IsNullOrWhiteSpace(sex))
				return false;

			// Convierte el sexo a minúsculas para hacer la comparación más fácil
			string sexLower = sex.ToLower();

			// Verifica si el sexo es válido
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
	}
}
