using System;
using System.Collections;
using Protocol;

namespace Server
{
    public static class Router
    {
        private static readonly ServerController serverController = new ServerController();

        public static void Handle(Connection conn)
        {
            string[][][] message = conn.ReadMessage();
            var request = new Request(message);

            switch (request.Command)
            {
                case Command.Login:
                    serverController.ConnectClient(conn, request);
                    break;
                default:
                    serverController.InvalidCommand(conn, request);
                    break;
            }
        }
    }
}