using System;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MedicalSystemCliente
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            using (ClientWebSocket clientWebSocket = new ClientWebSocket())
            {
                await clientWebSocket.ConnectAsync(new Uri("ws://localhost:1200"), CancellationToken.None);

                Console.WriteLine("Conectado al servidor WebSocket.");

                _ = Task.Run(async () =>
                {
                    while (true)
                    {
                        ArraySegment<byte> receiveBuffer = new ArraySegment<byte>(new byte[1024]);
                        WebSocketReceiveResult result = await clientWebSocket.ReceiveAsync(receiveBuffer, CancellationToken.None);
                        string receivedMessage = Encoding.UTF8.GetString(receiveBuffer.Array, 0, result.Count);
                        Console.WriteLine($"Actualización del servidor: {receivedMessage}");
                        await Task.Delay(2000);
                    }
                });

                while (true)
                {
                    Console.WriteLine("\u001b[Menú Principal:\u001b");
                    Console.WriteLine("1. Hacer Cita");
                    Console.WriteLine("2. Ver Citas");
                    Console.WriteLine("3. Buscar Paciente");
                    Console.WriteLine("4. Registrar Nuevo Paciente");
                    Console.WriteLine("5. Modificar/Cancelar Citas");
                    Console.WriteLine("6. Salir");
                    Console.Write("Seleccione una opción: ");

                    string input = Console.ReadLine();

                    if (input.ToLower() == "6" || input.ToLower() == "exit")
                        break;

                    switch (input)
                    {
                        case "1":
                            await HacerCita(clientWebSocket);
                            break;
                        case "2":
                            await VerCitas(clientWebSocket);
                            break;
                        case "3":
                            await BuscarPaciente(clientWebSocket);
                            break;
                        case "4":
                            await RegistrarNuevoPaciente(clientWebSocket);
                            break;
                        case "5":
                            await ModificarCancelarCitas(clientWebSocket);
                            break;
                        default:
                            Console.WriteLine("Opción no válida. Inténtalo de nuevo.");
                            break;
                    }
                }

                await clientWebSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Cerrando conexión", CancellationToken.None);
            }
        }

        static async Task HacerCita(ClientWebSocket clientWebSocket)
        {
            Console.WriteLine("----- Hacer Cita -----");
            Console.Write("Ingrese su nombre: ");
            string nombre = Console.ReadLine();
            Console.Write("Ingrese su cédula: ");
            string cedula = Console.ReadLine();
            Console.Write("Ingrese su número de teléfono: ");
            string telefono = Console.ReadLine();

            string mensaje = $"HacerCita|{nombre}|{cedula}|{telefono}";
            byte[] buffer = Encoding.UTF8.GetBytes(mensaje);
            await clientWebSocket.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, CancellationToken.None);
        }

        static async Task VerCitas(ClientWebSocket clientWebSocket)
        {
            Console.WriteLine("----- Ver Citas -----");
            string mensaje = "VerCitas";
            byte[] buffer = Encoding.UTF8.GetBytes(mensaje);
            await clientWebSocket.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, CancellationToken.None);
        }

        static async Task BuscarPaciente(ClientWebSocket clientWebSocket)
        {
            Console.WriteLine("----- Buscar Paciente -----");
            string mensaje = "BuscarPaciente";
            byte[] buffer = Encoding.UTF8.GetBytes(mensaje);
            await clientWebSocket.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, CancellationToken.None);
        }

        static async Task RegistrarNuevoPaciente(ClientWebSocket clientWebSocket)
        {
            Console.WriteLine("----- Registrar Nuevo Paciente -----");
            Console.Write("Ingrese el nombre del nuevo paciente: ");
            string nuevoNombre = Console.ReadLine();
            string mensaje = $"RegistrarNuevoPaciente|{nuevoNombre}";
            byte[] buffer = Encoding.UTF8.GetBytes(mensaje);
            await clientWebSocket.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, CancellationToken.None);
        }

        static async Task ModificarCancelarCitas(ClientWebSocket clientWebSocket)
        {
            Console.WriteLine("----- Modificar/Cancelar Citas -----");
            Console.Write("Ingrese el número de cita que desea modificar/cancelar: ");
            string numeroCita = Console.ReadLine();
            string mensaje = $"ModificarCancelarCitas|{numeroCita}";
            byte[] buffer = Encoding.UTF8.GetBytes(mensaje);
            await clientWebSocket.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, CancellationToken.None);
        }
    }
}