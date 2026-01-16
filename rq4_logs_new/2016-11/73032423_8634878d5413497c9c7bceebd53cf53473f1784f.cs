using System;
using System.Data.SqlClient;

namespace YouZhiWenJiao.Web
{
    public class CommonPage : System.Web.UI.Page
    {
        protected int pos = 0;
        protected SqlConnection sqlConn = null;
        protected SqlCommand sqlCmd = null;


        protected CommonPage()
        {
        }

        #region Web Form Designer generated code
        override protected void OnLoad(EventArgs e)
        {
            Response.CacheControl = "No-cache";
            #region 定义连接
            sqlConn = new System.Data.SqlClient.SqlConnection();
            sqlCmd = new System.Data.SqlClient.SqlCommand();
            this.sqlConn.ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString;
            sqlConn.Open();
            sqlCmd.Connection = this.sqlConn;
            #endregion
            base.OnLoad(e);
        }

        protected void ShowMenu(int iMenu)
        {

            Response.Write(@"
        <li><a href=""index.aspx""  " + (iMenu == 1 ? " class='cur' " : "") + @">首&nbsp;页</a></li>
        <li><a href=""news.aspx""  " + (iMenu == 2 ? " class='cur' " : " ") + @">新闻动态</a></li>
        <li><a href=""notice.aspx""  " + (iMenu == 3 ? " class='cur' " : "  ") + @">公示公告</a></li>
        <li><a href=""project.aspx"" " + (iMenu == 4 ? " class='cur' " : " ") + @">项目信息</a></li>
        <li><a href=""regula.aspx""  " + (iMenu == 5 ? " class='cur' " : "  ") + @">政策指导</a></li>
        <li><a href=""law.aspx""  " + (iMenu == 6 ? " class='cur' " : "  ") + @">法律法规</a></li>
        <li><a href=""case.aspx""  " + (iMenu == 7 ? " class='cur' " : "  ") + @">案例分析</a></li>
        <li><a href=""about.aspx""  " + (iMenu == 8 ? " class='cur' " : "  ") + @">关于我们</a></li>
        <li><a href=""petition.aspx""  " + (iMenu == 9 ? " class='cur' " : "  ") + @">信&nbsp;访</a></li>");
        }
        override protected void OnUnload(EventArgs e)
        {
            if (sqlConn != null)
                sqlConn.Close();
            base.OnUnload(e);
        }

        #endregion


        public bool IsInt(string number)
        {
            if (number == null)
                return false;
            if (number.Length == 0)
                return false;
            for (int i = 0; i < number.Length; i++)
            {
                char n = number[i];
                if (n > '9' || n < '0')
                    return false;
            }
            return true;
        }


        protected void Alert(string prompt)
        {
            string script = "<script type=\"text/javascript\">alert(\"" + prompt + "\");</script>";
            ClientScript.RegisterStartupScript(GetType(), "_STARTALERT", script);
        }

        static public int ToInt(object obj)
        {
            return ToInt(obj, 0);
        }

        static public int ToInt(object obj, int def)
        {
            try
            {
                return Int32.Parse(obj.ToString());
            }
            catch
            {
                return def;
            }
        }
        static public double ToDouble(object obj, double def)
        {
            try
            {
                return double.Parse(obj.ToString());
            }
            catch
            {
                return def;
            }
        }
        public static string ConvertToAlertSaveString(string strSource)
        {
            string strRet = strSource.Replace("'", "\"");

            strRet = strRet.Replace("<script>", "");
            strRet = strRet.Replace("</script>", "");
            strRet = strRet.Replace("&", "&");
            strRet = strRet.Replace("'", "''");
            strRet = strRet.Replace("*", "");
            strRet = strRet.Replace("\n", "<br/>");
            strRet = strRet.Replace("\r\n", "<br/>");
            strRet = strRet.Replace("select", "");
            strRet = strRet.Replace("insert", "");
            strRet = strRet.Replace("update", "");
            strRet = strRet.Replace("delete", "");
            strRet = strRet.Replace("create", "");
            strRet = strRet.Replace("drop", "");
            strRet = strRet.Replace("delcare", "");
            return strRet;
        }

        public static String javaScriptEscape(String input)
        {
            if (input == null)
            {
                return input;
            }
            System.Text.StringBuilder filtered = new System.Text.StringBuilder(input.Length);
            char prevChar = '\u0000';
            char c;
            for (int i = 0; i < input.Length; i++)
            {
                c = input[i];
                if (c == '"')
                {
                    filtered.Append("\\\"");
                }
                else if (c == '\'')
                {
                    filtered.Append("\\'");
                }
                else if (c == '\\')
                {
                    filtered.Append("\\\\");
                }
                else if (c == '\t')
                {
                    filtered.Append("\\t");
                }
                else if (c == '\n')
                {
                    if (prevChar != '\r')
                    {
                        filtered.Append("\\n");
                    }
                }
                else if (c == '\r')
                {
                    filtered.Append("\\n");
                }
                else if (c == '\f')
                {
                    filtered.Append("\\f");
                }
                else if (c == '/')
                {
                    filtered.Append("\\/");
                }
                else
                {
                    filtered.Append(c);
                }
                prevChar = c;
            }
            return filtered.ToString();
        }
    }
}