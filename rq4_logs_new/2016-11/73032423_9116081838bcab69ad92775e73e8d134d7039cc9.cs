using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using YouZhiWenJiao.Web.Manage.Entity;
using System.Web.UI.WebControls;


namespace YouZhiWenJiao.Web.Manage
{
	public partial class type : CommonPage
	{
		public int UniqueId = 0;
		private string categorylist = ((int)category.公司简介).ToString() + ',' + ((int)category.公司新闻).ToString() + ',' + ((int)category.教师书库).ToString() + ',' + ((int)category.园所装备).ToString() + ',' + ((int)category.园长书库).ToString() + ',' + ((int)category.资料下载).ToString();

		protected void Page_Load(object sender, EventArgs e)
		{
			if (Session["user"] == null)
			{
				Response.Redirect("login.aspx");
			}

			if (!this.IsPostBack)
			{
				string oldvar = ddlList.SelectedValue;
				ddlList.Items.Clear();
				ddlList.Items.Add(new ListItem("所有", "0"));
				sqlCmd.CommandText = @"
select 
id, 
description 
from category 
where id in (" + categorylist + ");";
				var rd = sqlCmd.ExecuteReader();
				while (rd.Read())
				{
					ddlList.Items.Add(new ListItem(rd[1].ToString(), rd[0].ToString()));
				}
				rd.Close();
				if (oldvar != "")
				{
					ddlList.SelectedValue = oldvar;
				}
			}
		}

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

			sqlCmd.CommandText = @"
select 
type.id as typeid,
type.categoryid as categoryid,
category.description as categorydescription,
type.description as typedescription
from
type
left join category on type.categoryid = category.id
where categoryid in (" + categorylist + ")";

			if (ddlList.SelectedValue != "0")
			{
				sqlCmd.CommandText += " and categoryid=" + ddlList.SelectedValue + ";";
			}

			DataTable dt = new DataTable();
			SQLiteDataAdapter da = new SQLiteDataAdapter(sqlCmd);
			da.Fill(dt);
			Information info = null;

			for (int i = 0; i < dt.Rows.Count; i++)
			{
				info = new Information();
				info.Number = (i + 1).ToString();
				info.ID = dt.Rows[i]["typeid"].ToString() + "|" + dt.Rows[i]["categoryid"].ToString();
				info.Type = dt.Rows[i]["typedescription"].ToString();
				info.Category = dt.Rows[i]["categorydescription"].ToString();

				li.Add(info);
			}

			return li;
		}

		protected void SubDelClick(object sender, System.EventArgs e)
		{
			string strDocumentSortIds = null;
			strDocumentSortIds = Request.Form["chkEleId"];
			if(strDocumentSortIds!= null)
			{
			string[] idlist = strDocumentSortIds.Split(',');

			foreach(var value in idlist)
			{
				string[] id = value.Split('|');
				sqlCmd.CommandText = "delete type where id = " + id[0] + " and categoryid = " + id[1] + ";";
				sqlCmd.ExecuteNonQuery();
				Alert("删除成功!");
				PageChanged(null, null);
			}
			}
		}

		protected void SubCreClick(object sender, System.EventArgs e)
		{
			Response.Redirect("type_edit.aspx", false);
		}
	}
}