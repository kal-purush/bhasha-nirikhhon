using Microsoft.ServiceModel.WebSockets;
using Microsoft.Web.WebSockets;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web;
using System.Web.Http;
using System.Web.Mvc;

namespace Web2ProjekatBackend.Controllers
{
    public class DatabaseNotificationController : ApiController
    {
        public HttpResponseMessage Get()
        {
            HttpContext.Current.AcceptWebSocketRequest(new ChatWebSocketHandler());
            return Request.CreateResponse(HttpStatusCode.SwitchingProtocols);
        }
        class ChatWebSocketHandler : Microsoft.Web.WebSockets.WebSocketHandler
        {

            public ChatWebSocketHandler()
            {
                SetupNotifier();
            }

            protected void SetupNotifier()
            {
                using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
                {
                    connection.Open();
                    using (SqlCommand command = new SqlCommand(@"SELECT [IdKorisnika] FROM [dbo].[Incidents]", connection))
                    {
                        command.Notification = null;
                        SqlDependency dependency = new SqlDependency(command);
                        dependency.OnChange += new OnChangeEventHandler(dependency_OnChange);
                        if (connection.State == ConnectionState.Closed)
                        {
                            connection.Open();
                        }
                        var reader = command.ExecuteReader();
                        reader.Close();
                    }
                }
            }

            private static WebSocketCollection _chatClients = new WebSocketCollection();

            public override void OnOpen()
            {
                _chatClients.Add(this);
            }

            public override void OnMessage(string msg)
            {

            }

            private void dependency_OnChange(object sender, SqlNotificationEventArgs e)
            {
                _chatClients.Broadcast(string.Format("Data changed on {0}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")));
                SetupNotifier();
            }
        }
    }
}