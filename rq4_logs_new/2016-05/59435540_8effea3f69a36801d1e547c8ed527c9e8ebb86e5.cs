namespace SeedRetriever
{
	using System;
	using System.Net;
	using System.Security.Cryptography.X509Certificates;

	/// <summary>
	/// http://stackoverflow.com/a/17033135/5499609
	/// </summary>
	public class CertificateWebClient : WebClient
	{
		private readonly X509Certificate2 certificate;

		public CertificateWebClient(X509Certificate2 certificate)
		{
			this.certificate = certificate;
		}

		protected override WebRequest GetWebRequest(Uri address)
		{
			var request = (HttpWebRequest)base.GetWebRequest(address);

			ServicePointManager.ServerCertificateValidationCallback = (obj, X509certificate, chain, errors) => true;

			request.ClientCertificates.Add(this.certificate);
			return request;
		}
	}
}