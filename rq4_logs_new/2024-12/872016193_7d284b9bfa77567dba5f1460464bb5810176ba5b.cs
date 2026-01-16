using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;

namespace PruebaServicioJuegoPassword
{    
    
    public class PruebaServicioChat
    {
        public class ServicioChatMock : ServidorPassword.IServicioChatCallback
        {
            public bool MensajeRecibido { get; set; }

            public void Responder(string respuesta)
            {
                MensajeRecibido = true;
            }
        }

        [TestClass]
        public class PruebaChat 
        {
            private static ServidorPassword.ServicioChatClient _servicioChat;
            private static ServicioChatMock ServicioChatMock;

            [ClassInitialize]
            public static void ConfigurarChat(TestContext contextoPrueba) 
            {
                ServicioChatMock = new ServicioChatMock();
                _servicioChat = new ServidorPassword.ServicioChatClient(new InstanceContext(ServicioChatMock));
                ServicioChatMock.MensajeRecibido = false;
            }

            [TestMethod]
            public async Task PruebaChatearExitosa() 
            {                
                string nombreUsuario = "Mario";
                string codigoPartida = "12345678";
                string mensaje = "hola";
                string chatMensaje = $"{nombreUsuario}:{codigoPartida}:{mensaje}";
                _servicioChat.Chatear(chatMensaje);
                await Task.Delay(5000);
                Assert.IsTrue(ServicioChatMock.MensajeRecibido);
            }

            [TestMethod]
            public void PruebaChatearFallida()
            {
                string nombreUsuario = "Mario";
                string codigoPartida = "12345678";
                string mensaje = "hola";
                string chatMensaje = $"{nombreUsuario}:{codigoPartida}:{mensaje}";
                _servicioChat.Chatear(chatMensaje);                
                Assert.IsFalse(ServicioChatMock.MensajeRecibido);
            }
        }
    }    
}