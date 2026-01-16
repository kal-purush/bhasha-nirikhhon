using System;
using RestSharp;
using RestSharp.Authenticators;
namespace RestComm
{
	public partial class Account{
		public Email SendEmail(string From,string To,string Body,string Subject){

			RestClient client = new RestClient (baseurl + "Accounts/" + Sid + "/Email"  + "/Messages");
			RestRequest request = new RestRequest (Method.POST);
			client.Authenticator = new HttpBasicAuthenticator (Sid, authtoken);
			request.AddParameter ("From", From);
			request.AddParameter ("To", To);
			request.AddParameter ("Body", Body);
			request.AddParameter ("Subject", Subject);
			return new Email (client, request);
		}
	}
		
	public class Email
	{
		
		private RestClient client;
		private RestRequest request;

		public Email(RestClient client,RestRequest request){

			this.request = request;
			this.client = client;

		}
		public void AddCC(string CC){
			request.AddParameter ("CC", CC);
		}
		public void AddBCC(string BCC){
			request.AddParameter ("BCC", BCC);
		}
		public void SendMail(){
			IRestResponse response = client.Execute (request);

			response.Content.GetAccountProperty ("From");//will throw an error if email is not sent ie. xml response is not in proper format
		}
	}
}
