using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Xsl;

namespace ProjecteFinal
{
    class Llistar
    {
        public Llistar()
        {

        }

        public static void ClientsOrdenatsProvincia(OracleDataSet dsOracle)
        {
            DataTable dataTable = dsOracle.CLIENTS;

            dataTable.DefaultView.Sort = "CODIPROVINCIA ASC, NOM ASC";
            dataTable = dataTable.DefaultView.ToTable();
            dataTable.Namespace = "";

            dataTable.WriteXml("llistatClientsProvincia.xml");

            ConvertToHTML("clientsProvincia.xsl", "llistatClientsProvincia.xml", "llistatClientsProvincia.html");
            OpenFileNavigator("llistatClientsProvincia.html");
        }

        public static void ClientsOrdenatsMunicipi(OracleDataSet dsOracle)
        {
            DataTable dataTable = dsOracle.CLIENTS;

            dataTable.DefaultView.Sort = "CODIMUNICIPI ASC, NOM ASC";
            dataTable = dataTable.DefaultView.ToTable();
            dataTable.Namespace = "";

            dataTable.WriteXml("llistatClientsMunicipi.xml");

            ConvertToHTML("clientsMunicipis.xsl", "llistatClientsMunicipi.xml", "llistatClientsMunicipi.html");
            OpenFileNavigator("llistatClientsMunicipi.html");
        }

        public static void ArticlesAll(OracleDataSet dsOracle)
        {
            DataTable dataTable = dsOracle.ARTICLES;

            dataTable.DefaultView.Sort = "DESCRIPCIO ASC";
            dataTable = dataTable.DefaultView.ToTable();
            dataTable.Namespace = "";

            dataTable.WriteXml("llistatArticles.xml");

            ConvertToHTML("articles.xsl", "llistatArticles.xml", "llistatArticles.html");
            OpenFileNavigator("llistatArticles.html");

        }

        public static void ArticlesOrdenatsVenuts(OracleDataSet dsOracle, OracleConnection cnOracle)
        {
            string query = "SELECT art.codi, art.DESCRIPCIO, art.QUANTITATSTOCK, art.PCOST, art.PVENDA, art.DESCOMPTE, sum(lf.QuantitatVenuda) FROM ARTICLES art, LINEASFACTURA lf WHERE art.codi = lf.CODIARTICLE group by codi, art.DESCRIPCIO, art.DESCOMPTE, art.PCOST, art.PVENDA, art.QUANTITATSTOCK ORDER BY sum(lf.QuantitatVenuda) DESC ";
            cnOracle.Open();
            DataTable dataTable = new DataTable();

            OracleCommand cmd = cnOracle.CreateCommand();
            cmd.CommandText = query;

            dataTable.Load(cmd.ExecuteReader());

            cnOracle.Close();

            dataTable.Namespace = "";
            dataTable.WriteXml("llistatArticlesPerMesVenuts.xml");

            ConvertToHTML("articles.xsl", "llistatArticlesPerMesVenuts.xml", "llistatArticlesPerMesVenuts.html");
            OpenFileNavigator("llistatArticlesPerMesVenuts.html");
        }

        public static void ArticlesOrdenatsEstoc(OracleDataSet dsOracle)
        {
            DataTable dataTable = dsOracle.ARTICLES;

            dataTable.DefaultView.Sort = "QUANTITATSTOCK ASC";
            dataTable = dataTable.DefaultView.ToTable();
            dataTable.Namespace = "";

            dataTable.WriteXml("llistatArticlesPerStock.xml");

            ConvertToHTML("articles.xsl", "llistatArticlesPerStock.xml", "llistatArticlesPerStock.html");
            OpenFileNavigator("llistatArticlesPerStock.html");
        }

        public static bool LlistarUnAlbara(OracleDataSet dsOracle, OracleConnection cnOracle, int nAlbara)
        {
            try
            {
                cnOracle.Open();
                OracleCommand cmd;
                OracleDataReader reader;
                DataSet albarans = new DataSet("Albarans");

                #region Empresa
                DataTable empresa = new DataTable("Empresa");
                empresa.Columns.Add("Logotip", typeof(string));
                empresa.Columns.Add("Nom", typeof(string));
                empresa.Columns.Add("Adreça", typeof(string));
                empresa.Columns.Add("NIF", typeof(string));
                empresa.Columns.Add("Telefon", typeof(string));
                empresa.Columns.Add("Fax", typeof(string));

                DataRow fila = empresa.NewRow();

                fila[0] = "logo.jpg";
                fila[1] = "Reparacions";
                fila[2] = "Adreça inventada";
                fila[3] = "NIF";
                fila[4] = "987654321";
                fila[5] = "Fax inventat";

                empresa.Rows.Add(fila);
                #endregion

                #region DataTable Capçalera
                DataTable cabAlbara = new DataTable("CabAlbara");
                cabAlbara.Columns.Add("NAlbara", typeof(Int32));
                cabAlbara.Columns.Add("DataAlbara", typeof(DateTime));
                cabAlbara.Columns.Add("NIF", typeof(string));
                cabAlbara.Columns.Add("Nom", typeof(string));
                cabAlbara.Columns.Add("Direccio", typeof(string));

                cmd = cnOracle.CreateCommand();
                cmd.CommandText = "SELECT nalbara, dataalbara, nif, nom, direccio FROM cabalbara WHERE nalbara = " + nAlbara;

                reader = cmd.ExecuteReader();

                if (reader.Read())
                {
                    fila = cabAlbara.NewRow();

                    fila[0] = reader.GetInt32(0);
                    fila[1] = reader.GetDateTime(1);
                    fila[2] = reader.GetString(2);
                    fila[3] = reader.GetString(3);
                    fila[4] = reader.GetString(4);

                    cabAlbara.Rows.Add(fila);
                    cmd.Dispose();
                    reader.Dispose();
                    reader.Close();
                    #endregion

                    #region DataTable Linia
                    DataTable liniaAlbara = new DataTable("LiniaAlbara");
                    liniaAlbara.Columns.Add("Descripcio", typeof(string));
                    liniaAlbara.Columns.Add("QuantitatVenuda", typeof(Int32));
                    liniaAlbara.Columns.Add("PreuVenda", typeof(double));
                    liniaAlbara.Columns.Add("Descompte", typeof(Int32));

                    cmd = cnOracle.CreateCommand();
                    cmd.CommandText = "SELECT descripcio, quantitatvenuda, preuvenda, descompte FROM lineasalbara WHERE nalbara = " + fila[0];

                    reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        fila = liniaAlbara.NewRow();

                        fila[0] = reader.GetOracleString(0);
                        fila[1] = reader.GetInt32(1);
                        fila[2] = reader.GetDouble(2);
                        if (reader["descompte"] != DBNull.Value) fila[3] = reader.GetInt32(3);
                        else fila[3] = 0;

                        liniaAlbara.Rows.Add(fila);
                    }
                    #endregion

                    albarans.Tables.Add(empresa);
                    albarans.Tables.Add(cabAlbara);
                    albarans.Tables.Add(liniaAlbara);

                    albarans.WriteXml("albarans.xml");

                    ConvertToHTML("albarans.xslt", "albarans.xml", "albarans.html");

                    OpenFileNavigator("albarans.html");

                    reader.Dispose();
                    reader.Close();
                    cmd.Dispose();

                    return true;
                }
                else
                {
                    cmd.Dispose();
                    reader.Dispose();
                    reader.Close();
                    return false;
                }
            }
            catch (Exception ex)
            {
                return false;
            }
            finally
            {
                cnOracle.Close();
            }
        }

        public static bool LlistarEntreAlbarans(OracleDataSet dsOracle, OracleConnection cnOracle, int valorInici, int valorFinal)
        {
            try
            {
                cnOracle.Open();
                OracleCommand cmd;
                OracleCommand cmdLinies;
                OracleDataReader reader;
                OracleDataReader readerLinies;
                DataSet albarans = new DataSet("Albarans");
                DataTable cabAlbara = new DataTable("CabAlbara");
                DataTable empresa = new DataTable("Empresa");
                DataTable liniaAlbara = new DataTable("LiniaAlbara");

                #region Empresa
                empresa.Columns.Add("Logotip", typeof(string));
                empresa.Columns.Add("Nom", typeof(string));
                empresa.Columns.Add("Adreça", typeof(string));
                empresa.Columns.Add("NIF", typeof(string));
                empresa.Columns.Add("Telefon", typeof(string));
                empresa.Columns.Add("Fax", typeof(string));

                DataRow fila = empresa.NewRow();

                fila[0] = "logo.jpg";
                fila[1] = "Reparacions";
                fila[2] = "Adreça inventada";
                fila[3] = "NIF";
                fila[4] = "987654321";
                fila[5] = "Fax inventat";

                empresa.Rows.Add(fila);
                #endregion

                #region DataTable Capçalera
                cabAlbara.Columns.Add("NAlbara", typeof(Int32));
                cabAlbara.Columns.Add("DataAlbara", typeof(DateTime));
                cabAlbara.Columns.Add("NIF", typeof(string));
                cabAlbara.Columns.Add("Nom", typeof(string));
                cabAlbara.Columns.Add("Direccio", typeof(string));

                liniaAlbara.Columns.Add("NAlbara", typeof(Int32));
                liniaAlbara.Columns.Add("Descripcio", typeof(string));
                liniaAlbara.Columns.Add("QuantitatVenuda", typeof(Int32));
                liniaAlbara.Columns.Add("PreuVenda", typeof(double));
                liniaAlbara.Columns.Add("Descompte", typeof(Int32));

                cmd = cnOracle.CreateCommand();
                cmd.CommandText = "SELECT nalbara, dataalbara, nif, nom, direccio FROM cabalbara WHERE nalbara BETWEEN " + valorInici + " AND " + valorFinal;

                reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    fila = cabAlbara.NewRow();

                    fila[0] = reader.GetInt32(0);
                    fila[1] = reader.GetDateTime(1);
                    fila[2] = reader.GetString(2);
                    fila[3] = reader.GetString(3);
                    fila[4] = reader.GetString(4);

                    cabAlbara.Rows.Add(fila);
                    #endregion

                    #region DataTable Linia
                    cmdLinies = cnOracle.CreateCommand();
                    cmdLinies.CommandText = "SELECT nalbara, descripcio, quantitatvenuda, preuvenda, descompte FROM lineasalbara WHERE nalbara = " + fila[0];

                    readerLinies = cmdLinies.ExecuteReader();

                    while (readerLinies.Read())
                    {
                        fila = liniaAlbara.NewRow();

                        fila[0] = readerLinies.GetInt32(0);
                        fila[1] = readerLinies.GetOracleString(1);
                        fila[2] = readerLinies.GetInt32(2);
                        fila[3] = readerLinies.GetDouble(3);
                        if (readerLinies["descompte"] != DBNull.Value) fila[4] = readerLinies.GetInt32(4);
                        else fila[4] = 0;

                        liniaAlbara.Rows.Add(fila);
                    }

                    readerLinies.Dispose();
                    readerLinies.Close();
                    cmdLinies.Dispose();
                    #endregion
                }

                albarans.Tables.Add(empresa);
                albarans.Tables.Add(cabAlbara);
                albarans.Tables.Add(liniaAlbara);

                albarans.WriteXml("varisalbarans.xml");

                ConvertToHTML("varisalbarans.xsl", "varisalbarans.xml", "varisalbarans.html");

                OpenFileNavigator("varisalbarans.html");

                reader.Dispose();
                reader.Close();
                cmd.Dispose();

                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
            finally
            {
                cnOracle.Close();
            }
        }

        public static bool LlistarUnaFactura(OracleDataSet dsOracle, OracleConnection cnOracle, int nFactura)
        {
            try
            {
                cnOracle.Open();
                OracleCommand cmd;
                OracleDataReader reader;
                DataSet factures = new DataSet("Factures");

                #region Empresa
                DataTable empresa = new DataTable("Empresa");
                empresa.Columns.Add("Logotip", typeof(string));
                empresa.Columns.Add("Nom", typeof(string));
                empresa.Columns.Add("Adreça", typeof(string));
                empresa.Columns.Add("NIF", typeof(string));
                empresa.Columns.Add("Telefon", typeof(string));
                empresa.Columns.Add("Fax", typeof(string));

                DataRow fila = empresa.NewRow();

                fila[0] = "logo.jpg";
                fila[1] = "Reparacions";
                fila[2] = "Adreça inventada";
                fila[3] = "NIF";
                fila[4] = "987654321";
                fila[5] = "Fax inventat";

                empresa.Rows.Add(fila);
                #endregion

                #region DataTable Capçalera
                DataTable cabFactura = new DataTable("CabFactura");
                cabFactura.Columns.Add("NFactura", typeof(Int32));
                cabFactura.Columns.Add("DataFactura", typeof(DateTime));
                cabFactura.Columns.Add("NIF", typeof(string));
                cabFactura.Columns.Add("Nom", typeof(string));
                cabFactura.Columns.Add("Direccio", typeof(string));

                cmd = cnOracle.CreateCommand();
                cmd.CommandText = "SELECT nfactura, datafactura, nif, nom, direccio FROM cabfacturas WHERE nfactura = " + nFactura;

                reader = cmd.ExecuteReader();

                if (reader.Read())
                {
                    fila = cabFactura.NewRow();

                    fila[0] = reader.GetInt32(0);
                    fila[1] = reader.GetDateTime(1);
                    fila[2] = reader.GetString(2);
                    fila[3] = reader.GetString(3);
                    fila[4] = reader.GetString(4);

                    cabFactura.Rows.Add(fila);
                    cmd.Dispose();
                    reader.Dispose();
                    reader.Close();
                    #endregion

                    #region DataTable Linia
                    DataTable liniaFactura = new DataTable("LiniaFactura");
                    liniaFactura.Columns.Add("Descripcio", typeof(string));
                    liniaFactura.Columns.Add("QuantitatVenuda", typeof(Int32));
                    liniaFactura.Columns.Add("PreuVenda", typeof(double));
                    liniaFactura.Columns.Add("Descompte", typeof(Int32));

                    cmd = cnOracle.CreateCommand();
                    cmd.CommandText = "SELECT descripcio, quantitatvenuda, preuvenda, descompte FROM lineasfactura WHERE nfactura = " + fila[0];

                    reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        fila = liniaFactura.NewRow();

                        fila[0] = reader.GetOracleString(0);
                        fila[1] = reader.GetInt32(1);
                        fila[2] = reader.GetDouble(2);
                        if (reader["descompte"] != DBNull.Value) fila[3] = reader.GetInt32(3);
                        else fila[3] = 0;

                        liniaFactura.Rows.Add(fila);
                    }
                    #endregion

                    factures.Tables.Add(empresa);
                    factures.Tables.Add(cabFactura);
                    factures.Tables.Add(liniaFactura);

                    factures.WriteXml("factures.xml");

                    ConvertToHTML("factures.xsl", "factures.xml", "factures.html");

                    OpenFileNavigator("factures.html");

                    reader.Dispose();
                    reader.Close();
                    cmd.Dispose();

                    return true;
                }
                else
                {
                    cmd.Dispose();
                    reader.Dispose();
                    reader.Close();
                    return false;
                }
            }
            catch (Exception ex)
            {
                return false;
            }
            finally
            {
                cnOracle.Close();
            }
        }

        /// <summary>
        /// Create the HTML File from a XML and XSL files.
        /// </summary>
        /// <param name="xslFile">Fitxer XSL</param>
        /// <param name="xmlFile">Fitxer XML</param>
        /// <param name="outputName">Nom del fitxer HTML de sortida</param>
        private static void ConvertToHTML(string xslFile, string xmlFile, string outputName)
        {
            var myXslTrans = new XslCompiledTransform();
            myXslTrans.Load(xslFile);
            myXslTrans.Transform(xmlFile, outputName);
        }
            
        /// <summary>
        /// Starts a process to Open a HTML file to Firefox
        /// </summary>
        /// <param name="fileName"></param>
        private static void OpenFileNavigator(string fileName)
        {
            ProcessStartInfo startInfo = new ProcessStartInfo();
            startInfo.FileName = "firefox.exe";
            startInfo.Arguments = fileName;
            Process.Start(startInfo);
        }
    }
}