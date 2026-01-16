using System;
using RestSharp;
using RestSharp.Authenticators;
using System.Collections.Generic;

namespace RestComm
{
	
	public partial class Account{
		public List<Client>  GetClientList(){
			RestClient client = new RestClient (baseurl+"Accounts/"+Sid+"/Client");
			RestRequest request = new RestRequest (Method.GET);
			client.Authenticator=new HttpBasicAuthenticator (Sid, authtoken);
			IRestResponse response = client.Execute (request);
			List<Client> clientslist = new List<Client> ();
			List<string> sidlist = response.Content.GetAccountsProperty ("Sid");
			int i = sidlist.Count;
			for (int j = 0; j < i; j++) {
				clientslist.Add (new Client (response.Content, j));

			}
			return clientslist;

		}

	}
	public class Client
	{
		string Sid;
		string DateCreated;
		string DateUpdated;
		string FriendlyName;
		string AccountSid;
		string ApiVersion;
		string Login;
		string Password;
		string Status;
		string VoiceUrl;
		string VoiceMethod;
		string VoiceFallbackUrl;
		string VoiceFallbackMethod;
		string VoiceApplication;
		string Uri;
		
		public Client (string responsecontent,int  clientno)
		{
			Sid = responsecontent.GetAccountsProperty ("Sid")[clientno];
			DateCreated = responsecontent.GetAccountsProperty ("DateCreated")[clientno];
			DateUpdated = responsecontent.GetAccountsProperty ("DateUpdated")[clientno];
			FriendlyName = responsecontent.GetAccountsProperty ("FriendlyName")[clientno];
			AccountSid = responsecontent.GetAccountsProperty ("AccountSid")[clientno];
			ApiVersion = responsecontent.GetAccountsProperty ("ApiVersion")[clientno];
			Login = responsecontent.GetAccountsProperty ("Login")[clientno];
			Password = responsecontent.GetAccountsProperty ("Password")[clientno];
			Status = responsecontent.GetAccountsProperty ("Status")[clientno];
			VoiceUrl = responsecontent.GetAccountsProperty ("VoiceUrl")[clientno];
			VoiceMethod = responsecontent.GetAccountsProperty ("VoiceMethod")[clientno];
			VoiceFallbackUrl = responsecontent.GetAccountsProperty ("VoiceFallbackUrl")[clientno];
			VoiceFallbackMethod = responsecontent.GetAccountsProperty ("VoiceFallbackMethod")[clientno];
			VoiceApplication = responsecontent.GetAccountsProperty ("VoiceApplication")[clientno];
			Uri = responsecontent.GetAccountsProperty ("Uri")[clientno];
		}
	}
}
