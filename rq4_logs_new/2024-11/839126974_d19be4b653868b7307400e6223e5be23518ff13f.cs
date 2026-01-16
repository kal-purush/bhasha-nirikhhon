using Aplication.Interfaces;
using Models;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Web;
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
    public class EnviarMail : System.Web.Services.WebService
    {
        private readonly ICompraService _compraService;
        private readonly IUsuarioService _usuarioService;

        public EnviarMail()
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

                string body = CrearMensajeResumenCompra(compra, usuario, detallesCompra);

                EnviarMailCompra(usuario.Email, body);

                return "Correo enviado exitosamente a: " + usuario.Email;
            }
            catch (Exception ex)
            {
                return "Error al enviar el correo: " + ex.Message;
            }
        }

        private string CrearMensajeResumenCompra(Models.Compra compra, Usuario usuario, List<DetalleCompra> detallesCompra)
        {
            // Ruta de la plantilla
            string templatePath = HttpContext.Current.Server.MapPath("~/Templates/ResumenCompraTemplate.html");
            string templateContent = File.ReadAllText(templatePath);

            // Reemplazar los marcadores de posición con la información del usuario y compra
            templateContent = templateContent.Replace("{{ID_USUARIO}}", compra.IdUsuario.ToString());
            templateContent = templateContent.Replace("{{NOMBRE}}", $"{usuario.Nombre} {usuario.Apellido}");
            templateContent = templateContent.Replace("{{FECHA_COMPRA}}", compra.FechaPago.ToString("dd/MM/yyyy"));
            templateContent = templateContent.Replace("{{TOTAL_COMPRA}}", compra.Total.ToString("C"));

            // Generar los detalles de la compra en formato HTML
            string detallesHtml = "";
            foreach (var detalle in detallesCompra)
            {
                detallesHtml += "<tr>";
                detallesHtml += $"<td>{detalle.NombreProducto}</td>";
                detallesHtml += $"<td>{detalle.Cantidad}</td>";
                detallesHtml += $"<td>{detalle.PrecioUnitario:C}</td>";
                detallesHtml += $"<td>{detalle.Subtotal:C}</td>";
                detallesHtml += "</tr>";
            }

            // Insertar los detalles generados en la plantilla
            templateContent = templateContent.Replace("{{DETALLES_COMPRA}}", detallesHtml);

            return templateContent;
        }

        private void EnviarMailCompra(string emailDestino, string body)
        {
            try
            {
                MailMessage mensaje = new MailMessage();
                mensaje.From = new MailAddress("noreply@hrhub.com", "HR Hub");
                mensaje.To.Add(emailDestino);

                mensaje.Subject = "Resumen de tu Compra en HR Hub";
                mensaje.Body = body;
                mensaje.IsBodyHtml = true;

                SmtpClient smtp = new SmtpClient("smtp.gmail.com");
                smtp.Port = 587;
                smtp.Credentials = new System.Net.NetworkCredential(
                    ConfigurationManager.AppSettings["EmailUser"],
                    ConfigurationManager.AppSettings["EmailPassword"]);
                smtp.EnableSsl = true;
                smtp.Send(mensaje);
            }
            catch (Exception ex)
            {
                throw new Exception("Error al enviar el correo: " + ex.Message);
            }
        }
    }
}