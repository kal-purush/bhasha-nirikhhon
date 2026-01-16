// using System;
// using System.Linq;
// using System.Collections;
// using System.Collections.Generic;
// using System.Threading.Tasks;
// using Chakra;
// using SystemConsole = System.Console; // To capture console
//
// namespace Chakra
// {
//   public class Program
//   {
//     public static async Task Main(string[] args)
//     {
//       int callbackPort = int.Parse(args[0]);
//       __SNIPPET__
//       await Task.Run(() =>
//       {
//         var socketClient = new SocketClient();
//         socketClient.StartClient(SocketConfig.GetServer(), callbackPort);
//         socketClient.SendData(string.Join(Environment.NewLine, Console.Lines));
//         socketClient.Close();
//       });
//     }
//   }
// }
//
//
// namespace Chakra
// {
//   public static class Console
//   {
//     // public static ErrorConsole Error = new ErrorConsole();
//     public static IList<string> Lines = new List<string>(); 
//     public static void WriteLine(string message)
//     {
//       Lines.Add(message);
//     }
//   }
// }