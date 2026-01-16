using System;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;


namespace TCC
{
    class daoPaciente
    {
        string strConn = System.IO.File.ReadAllText(@"sgiConn.txt");
        int pxId;
        int codUsuario;
        int codUsuarioPrAcesso;
        string senhaPrAcesso;

        public DataTable carregaPaciente()
        {
            DataTable dt = new DataTable();

            StringBuilder sbCmd = new StringBuilder();
            sbCmd.AppendLine(" SELECT ID, NOME, CPF, RG, DTNASC, FONE, SEXO, NACIONALIDADE, ENDERECEO, NUMERO, CEP, UF ");
            sbCmd.AppendLine(" FROM PACIENTES ");

            SqlConnection sqlConn = new SqlConnection(strConn);

            try
            {
                sqlConn.Open();
                SqlCommand cmd = new SqlCommand(sbCmd.ToString(), sqlConn);
                SqlDataReader dr = cmd.ExecuteReader();

                dt.Load(dr);
            }
            catch (SqlException ex)
            { }
            finally
            { sqlConn.Close(); }


            return dt;
        }

        public int proxId()
        {
            StringBuilder sbCmd = new StringBuilder();
            sbCmd.AppendLine(" SELECT ISNULL(MAX(ID),0)+1 AS IDMAX FROM PACIENTES ");

            SqlConnection sqlConn = new SqlConnection(strConn);

            sqlConn.Open();
            SqlCommand cmd = new SqlCommand(sbCmd.ToString(), sqlConn);
            SqlDataReader dr = cmd.ExecuteReader();

            while (dr.Read())
            {
                pxId = Convert.ToInt16(dr["IDMAX"].ToString());
            }

            return pxId;
        }

        public bool cadPaciente(string nome, string cpf, string rg, string dt, string fone, string sexo, string nac, string end, string num, string cep, string uf)
        {
            StringBuilder sbIn = new StringBuilder();
            sbIn.AppendLine(" INSERT INTO PACIENTES");
            sbIn.AppendLine(" (NOME, CPF, RG, DTNASC, FONE, SEXO, NACIONALIDADE, ENDERECEO, NUMERO, CEP, UF) ");
            sbIn.AppendLine(" VALUES ");
            sbIn.AppendLine("('" + nome + "','" + cpf + "','" + rg + "', CONVERT(date, '" + dt + "',103),'" + fone + "','" + sexo + "','" + nac + "','" + end + "','" + num + "','" + cep + "','" + uf + "');");

            SqlConnection sqlConn = new SqlConnection(strConn);

            try
            {
                sqlConn.Open();
                SqlCommand cmd = new SqlCommand(sbIn.ToString(), sqlConn);
                cmd.ExecuteNonQuery();
                MessageBox.Show("Inserido com Sucesso");
                return true;

            }
            catch (SqlException ex)
            {
                MessageBox.Show(ex.ToString());
            }
            finally
            {
                sqlConn.Close();
            }
            return false;

        }

        public DataTable consultaPaciente(int id)
        {
            DataTable dt = new DataTable();

            StringBuilder sbConsulta = new StringBuilder();
            sbConsulta.AppendLine(" SELECT ID, NOME, CPF, RG, DTNASC, FONE, SEXO, NACIONALIDADE, ENDERECEO, NUMERO, CEP, UF");
            sbConsulta.AppendLine(" FROM PACIENTES ");
            sbConsulta.AppendLine(" WHERE ID=" + id);

            SqlConnection sqlConn = new SqlConnection(strConn);

            try
            {
                sqlConn.Open();
                SqlCommand cmd = new SqlCommand(sbConsulta.ToString(), sqlConn);
                SqlDataReader dr = cmd.ExecuteReader();
                dt.Load(dr);
            }
            catch
            {

            }
            finally
            { sqlConn.Close(); }

            return dt;
        }

        public bool editPac(int id, string nome, string cpf, string rg, string dt, string fone, string sexo, string nac, string end, string num, string cep, string uf)
        {
            StringBuilder sbIn = new StringBuilder();
            sbIn.AppendLine(" UPDATE PACIENTES  SET ");
            sbIn.AppendLine(" NOME='" + nome + "'");
            sbIn.AppendLine(" ,CPF='" + cpf + "'");
            sbIn.AppendLine(" ,RG='" + rg + "'");
            sbIn.AppendLine(" ,DTNASC= CONVERT(DATE,'" + dt + "',103) ");
            sbIn.AppendLine(" ,FONE='" + fone + "'");
            sbIn.AppendLine(" ,SEXO='" + sexo + "'");
            sbIn.AppendLine(" ,NACIONALIDADE='" + nac + "'");
            sbIn.AppendLine(" ,ENDERECEO='" + end + "'");
            sbIn.AppendLine(" ,NUMERO='" + num + "'");
            sbIn.AppendLine(" ,CEP='" + cep + "'");
            sbIn.AppendLine(" ,UF='" + uf + "'");
            sbIn.AppendLine(" WHERE ID=" + id);

            SqlConnection sqlConn = new SqlConnection(strConn);

            try
            {
                sqlConn.Open();
                SqlCommand cmd = new SqlCommand(sbIn.ToString(), sqlConn);
                cmd.ExecuteNonQuery();
                MessageBox.Show("Inserido com Sucesso");
                return true;

            }
            catch (SqlException ex)
            {
                MessageBox.Show(ex.ToString());
            }
            finally
            {
                sqlConn.Close();
            }
            return false;
        }

        public bool delPac(int id)
        {
            StringBuilder sbDel = new StringBuilder();

            sbDel.AppendLine(" DELETE FROM PACIENTES ");
            sbDel.AppendLine(" WHERE ID= " + id);

            SqlConnection sqlConn = new SqlConnection(strConn);

            try
            {
                sqlConn.Open();
                SqlCommand cmd = new SqlCommand(sbDel.ToString(), sqlConn);
                cmd.ExecuteNonQuery();
                return true;
            }
            catch
            {
                return false;
            }
            finally
            {
                sqlConn.Close();
            }


        }

        public DataTable pesqPaciente(string id, string nome, string cpf)
        {
            DataTable dt = new DataTable();

            StringBuilder sbConsulta = new StringBuilder();
            sbConsulta.AppendLine(" SELECT ID, NOME,CPF ");
            sbConsulta.AppendLine(" FROM PACIENTES ");
            sbConsulta.AppendLine(" WHERE 0=0");
            sbConsulta.Append(!string.IsNullOrEmpty(id) ? " AND ID = '" + id + "' " : "");
            sbConsulta.Append(!string.IsNullOrEmpty(nome) ? " AND NOME LIKE '%" + nome + "%' " : "");
            sbConsulta.Append(!string.IsNullOrEmpty(cpf) ? " AND NOME LIKE '%" + cpf + "%' " : "");

            SqlConnection sqlConn = new SqlConnection(strConn);

            try
            {
                sqlConn.Open();
                SqlCommand cmd = new SqlCommand(sbConsulta.ToString(), sqlConn);
                SqlDataReader dr = cmd.ExecuteReader();
                dt.Load(dr);
            }
            catch
            {

            }
            finally
            { sqlConn.Close(); }

            return dt;
        }

        public int validaUsuario(string nome, string senha)
        {

            StringBuilder sbSel = new StringBuilder();
            sbSel.AppendLine(" SELECT * FROM USUARIOS ");
            sbSel.AppendLine(" WHERE 0=0 ");
            sbSel.AppendLine(" AND NOME =  '" + nome + "' ");
            sbSel.AppendLine(" AND SENHA =  '" + senha + "' ");

            SqlConnection sqlConn = new SqlConnection(strConn);

            try
            {
                sqlConn.Open();
                SqlCommand sqlCmd = new SqlCommand(sbSel.ToString(), sqlConn);

                SqlDataReader dr = sqlCmd.ExecuteReader();

                while (dr.Read())
                {
                    codUsuario = Convert.ToInt32(dr["IDUSUARIO"]);
                }
                return codUsuario;
            }
            catch
            {
                return 0;
            }
            finally
            {
                sqlConn.Close();
            }

        }

        public string[] primeiroAcesso(string nome, string senha, string psec, string rsec)
        {
            string[] retorno = new string[2];
            codUsuarioPrAcesso = 0;
            StringBuilder sbSel = new StringBuilder();
            sbSel.AppendLine(" SELECT IDUSUARIO,SENHA FROM USUARIOS ");
            sbSel.AppendLine(" WHERE NOME = '" + nome + "'");
            senhaPrAcesso = "";

            SqlConnection sqlConn = new SqlConnection(strConn);

            try
            {
                sqlConn.Open();
                SqlCommand sqlCmd = new SqlCommand(sbSel.ToString(), sqlConn);
                SqlDataReader dr = sqlCmd.ExecuteReader();
                while (dr.Read())
                {
                    codUsuarioPrAcesso = Convert.ToInt32(dr["IDUSUARIO"]);
                    senhaPrAcesso = dr["SENHA"].ToString();
                    int sh = senhaPrAcesso.Length;
                }
                sqlConn.Close();
                if (codUsuarioPrAcesso > 0 && !string.IsNullOrEmpty(senhaPrAcesso))
                {
                    retorno[0] = "0";
                    retorno[1] = "Usuário já tem senha!";
                    return retorno;
                }
                else
                {
                    if (codUsuarioPrAcesso > 0 && string.IsNullOrEmpty(senhaPrAcesso))
                    {
                        sbSel.Clear();
                        sbSel.AppendLine(" UPDATE USUARIOS SET ");
                        sbSel.AppendLine(" SENHA= '" + senha + "'");
                        sbSel.AppendLine(" ,PSEC= '" + psec + "'");
                        sbSel.AppendLine(" ,RSEC= '" + rsec + "'");
                        sbSel.AppendLine(" WHERE IDUSUARIO= '" + codUsuarioPrAcesso + "'");
                        try
                        {
                            //sqlConn.Open();
                            SqlCommand sqlUpdate = new SqlCommand(sbSel.ToString(), sqlConn);
                            sqlConn.Open();
                            sqlUpdate.ExecuteNonQuery();
                            sqlConn.Close();
                        }
                        catch
                        {
                            retorno[0] = "0";
                            retorno[1] = "Erro ao gravar dados";
                            return retorno;
                        }
                    }
                    else
                    {
                        retorno[0] = "0";
                        retorno[1] = "Não Cadastrado!";
                        return retorno;
                    }
                }
                retorno[0] = "1";
                retorno[1] = "Usuário Alterado!";
                return retorno;
            }
            catch
            {
                retorno[0] = "0";
                retorno[1] = "Erro ao gravar dados";
                return retorno;
            }
            finally
            {
                sqlConn.Close();
            }

        }

        public string[] recSenha(string nome)
        {
            string strCmd = "SELECT IDUSUARIO, PSEC FROM USUARIOS WHERE NOME ='" + nome + "'";
            string[] psec = new string[2];
            SqlConnection sqlConn = new SqlConnection(strConn);
            SqlCommand sqlCmd = new SqlCommand(strCmd, sqlConn);
            try
            {
                sqlConn.Open();
                SqlDataReader dr = sqlCmd.ExecuteReader();
                while (dr.Read())
                {
                    psec[0] = dr["PSEC"].ToString();
                    psec[1] = dr["IDUSUARIO"].ToString();
                }

            }
            catch
            {
                psec[0] = "Usuário Incorreto!";
                psec[1] = "0";
                return psec;
            }
            finally
            {
                sqlConn.Close();
            }
            if (string.IsNullOrEmpty(psec[0]))
            {
                psec[0] = "Usuário Incorreto!";
                psec[1] = "0";
                return psec;
            }

            return psec;
        }

        public string obtResp(int cod)
        {
            string resp = string.Empty;
            string strCmd = "SELECT RSEC FROM USUARIOS WHERE IDUSUARIO='" + cod + "'";
            SqlConnection sqlConn = new SqlConnection(strConn);
            try
            {
                sqlConn.Open();
                SqlCommand sqlCmd = new SqlCommand(strCmd, sqlConn);
                SqlDataReader dr = sqlCmd.ExecuteReader();
                while (dr.Read())
                {
                    resp = dr["RSEC"].ToString();
                }
                return resp;
            }
            catch { return "$$"; }
            finally { sqlConn.Close(); }

        }

        public bool alteraSenha(int cod, string senha)
        {
            SqlConnection sqlConn = new SqlConnection(strConn);
            string strCmd = "UPDATE USUARIOS SET SENHA ='" + senha + "'" +
                            "WHERE IDUSUARIO = '" + cod + "'";

            try
            {
                sqlConn.Open();
                SqlCommand sqlCmd = new SqlCommand(strCmd, sqlConn);
                sqlCmd.ExecuteNonQuery();
                return true;
            }
            catch
            { return false; }
            finally
            { sqlConn.Close(); }

        }

        public bool chkUser(string nomeComp, string nome, string tipo)
        {
            string user = "";
            string strCmd = "SELECT NOME FROM USUARIOS WHERE NOME ='" + nome + "'";
            SqlConnection sqlConn = new SqlConnection(strConn);
            try
            {
                sqlConn.Open();
                SqlCommand sqlCmd = new SqlCommand(strCmd, sqlConn);
                SqlDataReader dr = sqlCmd.ExecuteReader();
                while (dr.Read())
                {
                    user = dr["NOME"].ToString();
                }
                sqlConn.Close();


                if (user.Length > 0 || !string.IsNullOrEmpty(user))
                    return false;
                else
                {
                    sqlConn.Open();
                    SqlCommand sqlIns = new SqlCommand("INSERT INTO USUARIOS (NOME,NOME_COMPLETO,TIPO)"+
                        "VALUES ('" + nome + "','"+nomeComp+"','"+tipo+"');", sqlConn);
                    sqlIns.ExecuteNonQuery();
                    sqlConn.Close();

                    return true;
                }
            }
            catch
            {
                return false;
            }
            finally { sqlConn.Close(); }
        }

        public string userLog(int cod)
        {
            string ret = string.Empty;
            SqlConnection sqlConn = new SqlConnection(strConn);

            try
            {
                sqlConn.Open();
                SqlCommand sqlCmd = new SqlCommand("SELECT NOME_COMPLETO FROM USUARIOS WHERE IDUSUARIO='" + cod + "'",sqlConn);
                SqlDataReader dr = sqlCmd.ExecuteReader();
                while(dr.Read())
                {
                    ret = dr["NOME_COMPLETO"].ToString();
                }
            }
            catch{}
            finally { sqlConn.Close(); }

            return ("Olá, " + ret);
        }

        public DataTable carregaUsuario()
        {
            DataTable dt = new DataTable();

            StringBuilder sbCmd = new StringBuilder();
            sbCmd.AppendLine(" SELECT IDUSUARIO, NOME, SENHA, TIPO, DATAINI, DATAFIM, US_STATUS, PSEC, RSEC, NOME_COMPLETO ");
            sbCmd.AppendLine(" FROM USUARIOS ");

            SqlConnection sqlConn = new SqlConnection(strConn);

            try
            {
                sqlConn.Open();
                SqlCommand cmd = new SqlCommand(sbCmd.ToString(), sqlConn);
                SqlDataReader dr = cmd.ExecuteReader();

                dt.Load(dr);
            }
            catch (SqlException ex)
            { }
            finally
            { sqlConn.Close(); }


            return dt;
        }
    }
}