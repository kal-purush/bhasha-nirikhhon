using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Data.SQLite;
using System.Data;
using YouZhiWenJiao.Web.Common;

namespace YouZhiWenJiao.Web
{
	public partial class videos : CommonPage
	{
		public int UniqueId = 0;
		protected void Page_Load(object sender, EventArgs e)
		{
			//header.meauIndex = 2;
			if (Session["user"] == null)
			{
				Response.Redirect("login.aspx");
			}

			PageChanged(null, null);
		}

		protected void PageChanged(object sender, System.Web.UI.WebControls.DataGridPageChangedEventArgs e)
		{
			sqlCmd.CommandText = @"
select 
id,
title,
video,
date(datetime) as datetime
from product
where categoryid = @CategoryId order by datetime desc";
			sqlCmd.Parameters.Add("@CategoryId", DbType.Int16);
			sqlCmd.Parameters["@CategoryId"].Value = (int)category.资料下载;

			DataSet ds = new DataSet();
			SQLiteDataAdapter da = new SQLiteDataAdapter(sqlCmd);
			var dt = new DataTable();
			da.Fill(dt);
			int iAllCount = dt.Rows.Count;
			int iPageSize = rptDate2.PageSize;
			int iNum = iAllCount % iPageSize;
			rptDate2.DataSource = dt.DefaultView;
			rptDate2.DataBind();
		}
		protected void DataBindings(object sender, System.Web.UI.WebControls.RepeaterItemEventArgs e)
		{
			UniqueId++;
		}
	}
}