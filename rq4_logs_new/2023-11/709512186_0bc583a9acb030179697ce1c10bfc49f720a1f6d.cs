using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Net.Mail;
using System.Net;
using System.Text;
using MySql.Data.MySqlClient;
using TCC_euquero.Modelo;
using System.Web.UI.WebControls;

namespace TCC_euquero.Logica
{
    public class GerenciarCadastroUsuario : Banco
    {
        #region Variáveis

        List<Parametro> parametros = new List<Parametro>();
        int codigoValidacao = 0;
        string CSS = "color:green; font-size:30px;";

        #endregion


        #region Construtores

        public GerenciarCadastroUsuario()
        {
        }

        #endregion


        #region Métodos

        public bool VerificarDisponibilidadeEmail(string emailUsuario)
        {
            parametros.Clear();
            parametros.Add(new Parametro("pEmail", emailUsuario));
            MySqlDataReader dados = ConsultarComando($"select VerificarDisponibilidadeEmail('{emailUsuario}')");
            
            dados.Read();

            if (bool.Parse(dados[0].ToString()))
            {
                dados.Close();
                Desconectar();

                return true;
            }
            else
            {
                dados.Close();
                Desconectar();

                return false;
            }
        }

        private int ConsultarCodigoValidacao(string emailUsuario)
        {
            parametros.Clear();
            parametros.Add(new Parametro("pEmail", emailUsuario));
            MySqlDataReader dados = ConsultarProcedure("ConsultarCodigoValidacao", parametros);

            if (dados.Read())
                codigoValidacao = int.Parse(dados[0].ToString());
                dados.Close();

            Desconectar();

            return codigoValidacao;
        }

        public void EnviarCodigoEmail(string emailUsuario)
        {
            MailMessage mail = new MailMessage("leilaoEuQuero@outlook.pt", emailUsuario);
            EnvioDeEmail envioDeEmail = new EnvioDeEmail();

            codigoValidacao = ConsultarCodigoValidacao(emailUsuario);

            mail.Subject = $"Valide sua conta na EuQuero!";
            mail.IsBodyHtml = true;
            mail.Body = $@"     <html>
                                    <body>
                                        <div style={CSS}>
                                            <p> Você está a um passo de tornar sua conta válida! Para isso, insira o código abaixo na página do site que o solicita.</p>
                                            <p>{codigoValidacao}</p>
                                        </div>
                                    </body>
                                </html>";
            mail.SubjectEncoding = Encoding.GetEncoding("UTF-8");
            mail.BodyEncoding = Encoding.GetEncoding("UTF-8");

            envioDeEmail.Enviar(mail);
        }

        public string ValidarConta(string emailUsuario, int codigoDigitado)
        {
            codigoValidacao = ConsultarCodigoValidacao(emailUsuario);

            if(codigoValidacao == codigoDigitado)
            {
                parametros.Clear();
                parametros.Add(new Parametro("pEmail", emailUsuario));

                ExecutarProcedure("ValidarConta", parametros);

                return "Sua conta na EuQuero foi validada com sucesso!";
            }
            else
            {
                return "O código de validação está incorreto! Tente novamente.";
            }
        }

        #endregion
    }
}