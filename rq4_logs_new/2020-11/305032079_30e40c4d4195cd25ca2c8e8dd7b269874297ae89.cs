namespace Logic.Connection.Configuration {
	using System.Net;
	using TCPSocketClient;

	public abstract class TCPSocketConfiguration {
		public static void BuildDefaultConfiguration(out TCPSocket socket) {
			socket = null;
			using WebClient client = new WebClient();
			string address =
				client.DownloadString("https://amigosinformaticos.000webhostapp.com/");
			string serverAddress = address == "201.105.200.72" ? "192.168.1.99" : "201.105.200.72";
			socket = new TCPSocket(serverAddress, 42069);
		}
	}
}