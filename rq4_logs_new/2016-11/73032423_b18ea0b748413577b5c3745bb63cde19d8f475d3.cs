using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using YouZhiWenJiao.Web.Manage.Entity;

namespace YouZhiWenJiao.Web.Manage
{
    public partial class download : CommonPage
    {
        public int UniqueId = 0;
        //protected void Page_Load(object sender, EventArgs e)
        //{
        //    rptDate.DataSource = GetNewList();
        //    rptDate.DataBind();
        //}
        protected void PageChanged(object sender, System.Web.UI.WebControls.DataGridPageChangedEventArgs e)
        {
            rptDate.DataSource = GetNewList();
            rptDate.DataBind();
        }

        protected void DataBindings(object sender, System.Web.UI.WebControls.RepeaterItemEventArgs e)
        {
            UniqueId++;
        }
        public IList GetNewList()
        {
            IList li = new ArrayList();

            sqlCmd.CommandText = @"select id,title,updatedatetime from product where title like '@search' order by updatedatetime desc";

            if (!sqlCmd.Parameters.Contains("@search"))
            {
                sqlCmd.Parameters.Add("@search", DbType.String);
            }
            sqlCmd.Parameters["@search"].Value = "%" + txtserarch.Value + "%";

            //SQLiteParameter[] parameters = 
            //{
            //    new SQLiteParameter("@search", DbType.String)
            //};
            //parameters[0].Value = "%" + txtserarch.Value + "%";

            //var ds = SQLiteHelper.ExecuteDataSet(sqlConn, sqlCmd.CommandText, parameters);

            DataSet ds = new DataSet();
            SQLiteDataAdapter da = new SQLiteDataAdapter(sqlCmd);
            da.Fill(ds);
            DataTable dt = ds.Tables[0];
            Information info = null;

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                info = new Information();
                info.Number = (i + 1).ToString();
                info.ID = dt.Rows[i]["id"].ToString();
                info.Title = dt.Rows[i]["title"].ToString();
                info.DateTime = DateTime.Parse(dt.Rows[i]["Datetime"].ToString()).ToShortDateString();

                li.Add(info);
            }

            return li;
        }

        protected void SubDelClick(object sender, System.EventArgs e)
        {
            string strDocumentSortIds = null;
            strDocumentSortIds = Request.Form["chkEleId"];
            if (strDocumentSortIds != "" && strDocumentSortIds != null)
            {
                sqlCmd.CommandText = "update product set delect = 1 where id in(" + strDocumentSortIds + ")";
                sqlCmd.ExecuteNonQuery();
                Alert("删除成功!");
                PageChanged(null, null);
            }
        }

        protected void SubCreClick(object sender, System.EventArgs e)
        {
            Response.Redirect("about_info.aspx", false);
        }
    }
}