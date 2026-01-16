using Aplication.Interfaces;
using Aplication.Services;
using Models.Enums;
using System;
using System.Configuration;
using System.IO;
using System.Net.Mail;
using System.Web.Services;
using Unity;

namespace GUI.WebService
{
    /// <summary>
    /// Descripción breve de EnviarMail
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    public class MailService : System.Web.Services.WebService
    {
        private readonly ICompraService _compraService;
        private readonly IUsuarioService _usuarioService;

        public MailService()
        {
            _compraService = Global.Container.Resolve<ICompraService>();
            _usuarioService = Global.Container.Resolve<IUsuarioService>();
        }

        [WebMethod]
        public string EnviarResumenCompraPorEmail(int idCompra)
        {
            try
            {
                var compra = _compraService.ObtenerCompra(idCompra);
                if (compra == null) throw new Exception("Compra no encontrada.");

                var usuario = _usuarioService.ObtenerUsuarioPorId(compra.IdUsuario);
                if (usuario == null) throw new Exception("Usuario no encontrado.");

                var detallesCompra = _compraService.ObtenerDetalleCompra(idCompra);
                if (detallesCompra == null || detallesCompra.Count == 0)
                    throw new Exception("La compra no tiene productos.");

                string body = _compraService.CrearMensajeResumenCompra(compra, usuario, detallesCompra);
                GenerarPDFService generarPDFService = new GenerarPDFService();
                byte[] pdfBytes = generarPDFService.GenerarPdf(compra, usuario, detallesCompra);

                EnviarMail(usuario.Email, Models.Enums.AsuntoMail.CompraProductos, body, pdfBytes);
                return "Correo enviado exitosamente a: " + usuario.Email;
            }
            catch (Exception ex)
            {
                return "Error al enviar el correo: " + ex.Message;
            }
        }

        [WebMethod]
        public void EnviarMail(string email, AsuntoMail asuntoMail, string body, byte[] attachment = null)
        {
            try
            {
                MailMessage mensaje = new MailMessage();
                mensaje.From = new MailAddress("noreply@hrhub.com", "HR Hub");
                mensaje.To.Add(email);

                mensaje.Subject = ObtenerAsuntoCorreo(asuntoMail);
                mensaje.Body = body;
                mensaje.IsBodyHtml = true;

                if (attachment != null)
                {
                    using (MemoryStream ms = new MemoryStream(attachment))
                    {
                        Attachment pdfAttachment = new Attachment(ms, "ResumenCompra.pdf", "application/pdf");
                        mensaje.Attachments.Add(pdfAttachment);

                        SmtpClient smtp = new SmtpClient("smtp.gmail.com");
                        smtp.Port = 587;
                        smtp.Credentials = new System.Net.NetworkCredential(ConfigurationManager.AppSettings["EmailUser"], ConfigurationManager.AppSettings["EmailPassword"]);
                        smtp.EnableSsl = true;
                        smtp.Send(mensaje);
                    }
                }
                else
                {
                    SmtpClient smtp = new SmtpClient("smtp.gmail.com");
                    smtp.Port = 587;
                    smtp.Credentials = new System.Net.NetworkCredential(ConfigurationManager.AppSettings["EmailUser"], ConfigurationManager.AppSettings["EmailPassword"]);
                    smtp.EnableSsl = true;
                    smtp.Send(mensaje);
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        private string ObtenerAsuntoCorreo(AsuntoMail asuntoMail)
        {
            switch (asuntoMail)
            {
                case AsuntoMail.RecuperacionContraseña:
                    return "Recuperación de contraseña";
                case AsuntoMail.GeneracionContraseña:
                    return "Generación de contraseña";
                case AsuntoMail.CompraProductos:
                    return "Compra de productos";
                default:
                    throw new ArgumentException("Tipo de asunto no válido");
            }
        }
    }
}