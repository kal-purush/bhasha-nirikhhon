using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Web;
using TCC_euquero.Modelo;
using TCC_euquero.Logica;

namespace TCC_euquero.Logica
{
    public class EnvioDeEmail : Banco
    {
        public void Enviar(MailMessage mail)
        {
            SmtpClient smtpClient = new SmtpClient("smtp.office365.com", 587);
            smtpClient.UseDefaultCredentials = false;
            smtpClient.Credentials = new NetworkCredential("leilaoEuQuero@outlook.pt", "aafjkrTCC");
            smtpClient.EnableSsl = true;
            smtpClient.Send(mail);
        }
        public void CodigoValidacao(string emailUsuario)
        {
            MailMessage mail = new MailMessage("leilaoEuQuero@outlook.pt", emailUsuario);
            GerenciarCadastroUsuario gerenciarCadastroUsuario = new GerenciarCadastroUsuario();

            int codigoValidacao = gerenciarCadastroUsuario.ConsultarCodigoValidacao(emailUsuario);

            mail.Subject = $"Valide sua conta na EuQuero!";
            mail.IsBodyHtml = true;
            mail.Body = $@"     <html>
                                    <body>
                                        <div>
                                            <p> Você está a um passo de tornar sua conta válida! Para isso, insira o código abaixo na página do site que o solicita.</p>
                                            <p>{codigoValidacao}</p>
                                        </div>
                                    </body>
                                </html>";
            mail.SubjectEncoding = Encoding.GetEncoding("UTF-8");
            mail.BodyEncoding = Encoding.GetEncoding("UTF-8");

            Enviar(mail);
        }

        public void NotificarVitoria(int pAnuncio)
        {
            string emailComprador = "";
            string nomeAnuncio = "";
            string numeroVendedor = "";
            string nomeVendedor = "";

            List<Parametro> parametros = new List<Parametro>();
            parametros.Add(new Parametro("pCodigoAnuncio", pAnuncio.ToString()));

            MySqlDataReader dados = ConsultarProcedure("BuscarDadosMinimosCompradorVendedor", parametros);

            if (dados.Read())
            {
                emailComprador = dados["EmailComprador"].ToString();
                nomeVendedor = dados["NomeVendedor"].ToString();
                numeroVendedor = dados["TelefoneVendedor"].ToString();
                nomeAnuncio = dados["NomeAnuncio"].ToString();
            }

            dados.Close();
            Desconectar();

            if (String.IsNullOrEmpty(emailComprador))
            {
                return;
            }
            else
            {
                MailMessage mail = new MailMessage("leilaoEuQuero@outlook.pt", emailComprador);

                mail.Subject = $"Você foi o vencedor de um leilão!";
                mail.IsBodyHtml = true;
                mail.Body = $@"     <html>
                                    <body>
                                        <div>
                                            <p>Parabéns pela conquista! O seu lance foi o maior do anúncio: {nomeAnuncio}</p>
                                            <p>Telefone do vendedor: {numeroVendedor}</p>
                                            <p>Nome do vendedor: {nomeVendedor}</p>
                                        </div>
                                    </body>
                                </html>";
                mail.SubjectEncoding = Encoding.GetEncoding("UTF-8");
                mail.BodyEncoding = Encoding.GetEncoding("UTF-8");

                Enviar(mail);
            }
        }

        public void NotificarSaldoRessarcido(int pAnuncio, decimal pValor)
        {
            string emailUsuario = "";
            string nomeAnuncio = "";

            List<Parametro> parametros = new List<Parametro>();
            parametros.Add(new Parametro("pCodigoAnuncio", pAnuncio.ToString()));
            parametros.Add(new Parametro("pValorLance", pValor.ToString().Replace(',', '.')));

            MySqlDataReader dados = ConsultarProcedure("BuscarEmailUsuarioENomeProduto", parametros);

            if (dados.Read())
            {
                emailUsuario = dados["EmailUsuario"].ToString();
                nomeAnuncio = dados["NomeProduto"].ToString();
            }

            dados.Close();
            Desconectar();

            if (String.IsNullOrEmpty(emailUsuario))
            {
                return;
            }
            else
            {
                MailMessage mail = new MailMessage("leilaoEuQuero@outlook.pt", emailUsuario);

                mail.Subject = $"Saldo Ressarcido!";
                mail.IsBodyHtml = true;
                mail.Body = $@"     <html>
                                        <body>
                                            <div>
                                                <p>O valor {pValor.ToString("C", new CultureInfo("pt-br"))} foi devolvido para a carteira da sua conta EuQuero, pois seu lance foi superado no anúncio:</p>
                                                <p>{nomeAnuncio}.</p>
                                            </div>
                                        </body>
                                    </html>";

                mail.SubjectEncoding = Encoding.GetEncoding("UTF-8");
                mail.BodyEncoding = Encoding.GetEncoding("UTF-8");

                Enviar(mail);
            }
        }
    }
}