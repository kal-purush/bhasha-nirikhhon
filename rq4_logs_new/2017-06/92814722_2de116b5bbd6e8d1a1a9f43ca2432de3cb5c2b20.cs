using System;
using RestComm;
namespace Accounts
{
	class MainClass
	{
		public static void Main (string[] args)
		{
			//Login 
			Account akount = new Account ("AC13b4372c92ed5c07d951cf842e2664ff", "eff8eb1e1334884cf7dd59d3a00e687a");

			//prints Sid of Account
			Console.WriteLine (akount.Properties.Sid);

			//Creates Subaccount
			SubAccount subaccount=akount.CreateSubAccount("DemoAccounts","demoaccounts@restcomm.com","Demo@123");

			//prints name of subaccount
			Console.WriteLine(subaccount.Properties.friendlyname);

			//Changes the password
			subaccount.ChangePassword("Demo@123456");

			//Closes the subaccount
			subaccount.CloseSubAccount();
		}
	}
}