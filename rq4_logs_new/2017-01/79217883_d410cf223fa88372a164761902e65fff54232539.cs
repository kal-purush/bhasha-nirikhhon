using System;
using System.Net.Http.Headers;
using System.Text;

namespace RevStackCore.Cryptography
{
	
	public static class Base64
	{
		public static string Encrypt(string text)
		{
			var plainTextBytes = Encoding.UTF8.GetBytes(text);
			return Convert.ToBase64String(plainTextBytes);
		}

		public static string EncryptCredentials(string username,string password)
		{
			string text = username + ":" + password;
			return Encrypt(text);
		}

		public static string EncryptCredentials(Base64Credentials credentials)
		{
			string username = credentials.Username;
			string password = credentials.Password;
			return EncryptCredentials(username, password);
		}

		public static string Decrypt(string base64Value)
		{
			var encoding = Encoding.GetEncoding("iso-8859-1");
			return encoding.GetString(Convert.FromBase64String(base64Value));
		}

		public static Base64Credentials DecryptCredentials(string base64Value)
		{
			var credentials = Decrypt(base64Value);
			var split = credentials.Split(':');
			string username = split[0];
			string password = split[1];
			var result = new Base64Credentials
			{
				Username = username,
				Password = password
			};

			return result;
		}

		public static Base64Credentials DecryptCredentials(AuthenticationHeaderValue header)
		{
			var rawCredentials = header.Parameter;
			return DecryptCredentials(rawCredentials);
		}


	}
}