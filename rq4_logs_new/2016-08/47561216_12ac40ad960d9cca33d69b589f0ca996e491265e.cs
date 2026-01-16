using System;
using IO.Swagger.Api;
using IO.Swagger.Client;
using IO.Swagger.Model;
using Newtonsoft.Json;
using RestSharp;
using System.Security.Cryptography.X509Certificates;
using System.Collections.Generic;
using System.Net;

namespace CSharpSwagger
{
	class MainClass
	{
		static ApiClient client = new ApiClient("https://bgtest.landregistry.gov.uk/api/deeds");

		public static void Main (string[] args)
		{
			ServicePointManager.ServerCertificateValidationCallback += (sender, cert, chain, sslPolicyErrors) => true;

			if (args.Length > 0 && args[0].ToUpper() == "TRUE") {
				// Copy your security certificate into the bin/debug directory
				X509Certificate2 certificate = new X509Certificate2("./BGTEST-CERT.pfx", "password");
				RestClient restClient = client.RestClient;
				restClient.ClientCertificates = new X509CertificateCollection();
				restClient.ClientCertificates.Add (certificate);
			}


			// Give the deed a Title Number and MdRef
			string titleNumber = "DT567568";
			string mdRef = "e-MD12344";
			string property_address = "30 the gardens, the city, post cde";
			string identity_checked = "Y";

			// create at least one borrower
			PrivateIndividualName borrower = new PrivateIndividualName ("Paul","M","Smythe","01/10/1976",PrivateIndividualName.GenderEnum.Male,"07503254062","2 The Street, Plymouth, PL1 2PP");

			// add the borrower(s) to a list of borrowers (min 1)
			Borrowers borrowerList = new Borrowers();
			borrowerList.Add(borrower);

			string deedToken = PostDeed (titleNumber, mdRef, borrowerList, identity_checked, property_address);

			Console.WriteLine ("POST Complete - attempting GET");

			GetDeed(deedToken);

			Console.WriteLine("Process Complete");
			Console.ReadLine();
		}

		private static OperativeDeed GetDeed(string token){
			DeedApi deedGet = new DeedApi(new Configuration(client));

			Console.WriteLine("Retrieving Newly Posted Deed");

			OperativeDeed operativeDeed = deedGet.DeedDeedReferenceGet(token);
			Console.Write(operativeDeed.ToJson());

			return operativeDeed;
		}

		private static string PostDeed(string titleNumber, string mdRef, Borrowers borrowerList,
		                               string property_address, string identity_checked) {
			DefaultApi deedPost = new DefaultApi(new Configuration(client));
			DeedApplication deedApp = new DeedApplication(titleNumber,borrowerList,mdRef,property_address,identity_checked);

			deedApp.Borrowers = borrowerList;

			Console.WriteLine ("Sending POST to API");
			string result = deedPost.AddDeed(deedApp);
			Console.WriteLine ("New Deed created and accessible at : " + result);

			int start = result.LastIndexOf ('/')+1;
			int end = result.LastIndexOf ('"');
			string token = result.Substring (start, end-start);
			Console.WriteLine("Token for new deed is " + token);

			return token;
		}
	}
}