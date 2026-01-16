using Google.Cloud.SecretManager.V1;

namespace PMCore.Helpers
{
	public class AccessSecretVersion
	{
		public static string Get(string projectId, string secretId, string secretVersionId = "last")
		{
			// Create the client.
			SecretManagerServiceClient client = SecretManagerServiceClient.Create();

			// Build the resource name.
			SecretVersionName secretVersionName = new SecretVersionName(projectId, secretId, secretVersionId);

			// Call the API.
			AccessSecretVersionResponse result = client.AccessSecretVersion(secretVersionName);

			// Convert the payload to a string. Payloads are bytes by default.
			string payload = result.Payload.Data.ToStringUtf8();
			return payload;
		}
	}
}