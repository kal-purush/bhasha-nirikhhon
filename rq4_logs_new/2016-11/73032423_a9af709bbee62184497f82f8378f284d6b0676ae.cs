using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using YouZhiWenJiao.Web.Manage.Entity;

namespace YouZhiWenJiao.Web.Manage
{
	public partial class productimages : CommonPage
	{
		protected void Page_Load(object sender, EventArgs e)
		{
			if (Session["user"] == null)
			{
				Response.Redirect("login.aspx");
			}
		}

		protected void SubRepClick1(object sender, System.EventArgs e)
		{
			if (InputFile1.Value != "")
			{
				try
				{
					string path = Server.MapPath("../productimages/product1.jpg");
					InputFile1.PostedFile.SaveAs(path);
				}
				catch (Exception ex)
				{
					Response.Write(ex);
				}
			}
			Alert("替换成功!");
			Page_Load(null, null);
		}

		protected void SubRepClick2(object sender, System.EventArgs e)
		{
			if (InputFile2.Value != "")
			{
				try
				{
					string path = Server.MapPath("../productimages/product2.jpg");
					InputFile2.PostedFile.SaveAs(path);
				}
				catch (Exception ex)
				{
					Response.Write(ex);
				}
			}
			Alert("替换成功!");
			Page_Load(null, null);
		}

		protected void SubRepClick3(object sender, System.EventArgs e)
		{
			if (InputFile3.Value != "")
			{
				try
				{
					string path = Server.MapPath("../productimages/product3.jpg");
					InputFile3.PostedFile.SaveAs(path);
				}
				catch (Exception ex)
				{
					Response.Write(ex);
				}
			}
			Alert("替换成功!");
			Page_Load(null, null);
		}

		protected void SubRepClick4(object sender, System.EventArgs e)
		{
			if (InputFile4.Value != "")
			{
				try
				{
					string path = Server.MapPath("../productimages/product4.jpg");
					InputFile4.PostedFile.SaveAs(path);
				}
				catch (Exception ex)
				{
					Response.Write(ex);
				}
			}
			Alert("替换成功!");
			Page_Load(null, null);
		}
	}
}