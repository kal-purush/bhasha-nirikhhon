using System;
using RestSharp;
using RestSharp.Authenticators;
using System.Collections.Generic;
using System.Net;
using System.IO;
namespace RestComm
{
	
	public partial class Account{
		public makecall MakeCall(string From,string To,string Url){
			RestClient client=new RestClient(baseurl+"Accounts/"+Sid+"/Calls");
			RestRequest makecall = new RestRequest (Method.POST);
			client.Authenticator =new HttpBasicAuthenticator(Sid, authtoken);
			makecall.AddParameter ("From", From);
			makecall.AddParameter ("To", To);
			makecall.AddParameter ("Url", Url);

			return new makecall (client,makecall);

		}
		public CallFilter GetCallDetail(){
			
			RestRequest makecall = new RestRequest (Method.POST);

			makecall.Credentials = new NetworkCredential (Sid, authtoken);
			return new CallFilter (makecall,Sid);

		}



	}
	public class CallFilter{
		RestClient client;
		RestRequest request;
		string Sid;
		List<string> parametername=new List<string>();
		List<string> parametervalue=new List<string>();
		public CallFilter(RestRequest request,string Sid){
			
			this.request = request;
			this.Sid = Sid;
		}
		public void AddSearchFilter(string ParameterName,string ParameterValue){
			parametername.Add (ParameterName);
			parametervalue.Add (ParameterValue);
		}
		public List<Call> Search(){
			string clienturl=Account.baseurl+"Accounts/"+Sid+"/Calls";
			if (parametername.Count == 0) {
				clienturl += "?";
				int i = 0;
				foreach (string s in parametername) {
					if(i!=0)
						clienturl+="&";
					clienturl += parametername [i];
					clienturl += "=" + parametervalue;
					i++;
				}


			}
			client = new RestClient (clienturl);
			IRestResponse response = client.Execute (request);
			List<Call> calllist=new List<Call>();
			List<string> Sidlist = response.Content.GetAccountsProperty ("Sid");
			int count = Sidlist.Count;
			for (int j = 0; j < count; j++) {

				calllist.Add (new Call (response.Content, j));

			}
			return calllist;
		}
	}

	public class makecall{
		
		RestClient Client;
		RestRequest Request;
		public makecall(RestClient client,RestRequest request){
			Client = client;

			Request=request;

		}
		public void AddParameter(string ParameterName,string ParameterValue){
			
			Request.AddParameter (ParameterName, ParameterValue);

		}
		public Call call(){
			IRestResponse response = Client.Execute (Request);
			Console.WriteLine (response.Content);
			return new Call(response.Content);
		}


	}

	public class Call{
		#pragma warning disable 0414
		
		public string Sid;
		public string ParentCallSid;
		public string DateCreated;
		public string DateUpdated;
		public string To;
		public string From;
		public string PhoneNumberSid;
		public string Status;
		public string StartTime;
		public string EndTime;
		public string Duration;
		public string Price;
		public string Direction;
		public string AnsweredBy;
		public string ApiVersion;
		public string ForwardFrom;
		public string CallerName;
		public string Uri;

		public Call(string xmlresponse){
			Sid = xmlresponse.GetAccountProperty ("Sid");
			ParentCallSid = xmlresponse.GetAccountProperty ("ParentCallSid");
			DateCreated = xmlresponse.GetAccountProperty ("DateCreated");
			DateUpdated = xmlresponse.GetAccountProperty ("DateUpdated");
			To = xmlresponse.GetAccountProperty ("To");
			From = xmlresponse.GetAccountProperty ("From");
			PhoneNumberSid = xmlresponse.GetAccountProperty ("PhoneNumberSid");
			Status = xmlresponse.GetAccountProperty ("Status");
			StartTime = xmlresponse.GetAccountProperty ("StartTime");
			EndTime = xmlresponse.GetAccountProperty ("EndTime");
			Duration = xmlresponse.GetAccountProperty ("Duration");
			Price = xmlresponse.GetAccountProperty ("Price");
			Direction = xmlresponse.GetAccountProperty ("Direction");
			AnsweredBy = xmlresponse.GetAccountProperty ("AnsweredBy");
			ApiVersion = xmlresponse.GetAccountProperty ("ApiVersion");
			ForwardFrom = xmlresponse.GetAccountProperty ("ForwardFrom");
			CallerName = xmlresponse.GetAccountProperty ("CallerName");
			Uri = xmlresponse.GetAccountProperty ("Uri");
		}
		//use this constructor when there is list of call in response
		public Call(string xmlresponse,int elementunmber){

			Sid = xmlresponse.GetAccountsProperty ("Sid")[elementunmber];
			ParentCallSid = xmlresponse.GetAccountsProperty ("ParentCallSid")[elementunmber];
			DateCreated = xmlresponse.GetAccountsProperty ("DateCreated")[elementunmber];
			DateUpdated = xmlresponse.GetAccountsProperty ("DateUpdated")[elementunmber];
			To = xmlresponse.GetAccountsProperty ("To")[elementunmber];
			From = xmlresponse.GetAccountsProperty ("From")[elementunmber];
			PhoneNumberSid = xmlresponse.GetAccountsProperty ("PhoneNumberSid")[elementunmber];
			Status = xmlresponse.GetAccountsProperty ("Status")[elementunmber];
			StartTime = xmlresponse.GetAccountsProperty ("StartTime")[elementunmber];
			EndTime = xmlresponse.GetAccountsProperty ("EndTime")[elementunmber];
			Duration = xmlresponse.GetAccountsProperty ("Duration")[elementunmber];
			Price = xmlresponse.GetAccountsProperty ("Price")[elementunmber];
			Direction = xmlresponse.GetAccountsProperty ("Direction")[elementunmber];
			AnsweredBy = xmlresponse.GetAccountsProperty ("AnsweredBy")[elementunmber];
			ApiVersion = xmlresponse.GetAccountsProperty ("ApiVersion")[elementunmber];
			ForwardFrom = xmlresponse.GetAccountsProperty ("ForwardFrom")[elementunmber];
			CallerName = xmlresponse.GetAccountsProperty ("CallerName")[elementunmber];
			Uri = xmlresponse.GetAccountsProperty ("Uri")[elementunmber];







		}
	}
}
