using System.Net;
using System.Text;

HttpListener listener = new HttpListener();
listener.Prefixes.Add("http://localhost:8000/");

listener.Start();

Console.WriteLine("Listening...");

HttpListenerContext context = listener.GetContext();
HttpListenerRequest request = context.Request;

HttpListenerResponse response = context.Response;

Console.WriteLine(request.Url.ToString());
Console.WriteLine(request.HttpMethod);
Console.WriteLine(request.UserHostName);
Console.WriteLine(request.UserAgent);

string responseString = "Hello World!";
byte[] buffer = Encoding.UTF8.GetBytes(responseString);

response.ContentLength64 = responseString.Length;

System.IO.Stream output = response.OutputStream;

output.Write(buffer, 0, buffer.Length);

output.Close();

listener.Stop();
