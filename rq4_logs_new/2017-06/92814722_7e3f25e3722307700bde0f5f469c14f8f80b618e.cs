using System;
using RestComm;

class MainClass
{
	public static void Main(string[] args)
	{
		string Sid = Console.ReadLine();
		string Authtoken = Console.ReadLine ();
		Account account = new Account (Sid, Authtoken);

		//changes the password of account
		account.ChangePassword("Test@123");

		//Creates a subaccount
		SubAccount subaccount= account.CreateSubAccount ("Test", "demo@demo.com", "Demo@1234");

		//Returns List of All SubAccount
		List<subaccount> subaccountlist= account.GetSubAccountList ();
		//Prints Sid of Each SubAccount
		foreach (subaccount s in subaccountlist) {

			Console.WriteLine (s.Properties.Sid);
		}
		//close Selected Subaccount
		subaccount.CloseSubAcount;
	}

}