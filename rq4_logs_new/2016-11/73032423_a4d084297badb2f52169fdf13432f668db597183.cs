using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Data.SQLite;
using System.Data;
using YouZhiWenJiao.Web.Common;
using System.Text.RegularExpressions; 

namespace YouZhiWenJiao.Web
{
	public partial class search : CommonPage
	{
        protected List<CommonModel> CorporateNewsList;
        protected CommonTypeModel CorporateNewsType;
        protected string SearchWD;

		protected void Page_Load(object sender, EventArgs e)
		{
            CorporateNewsList = new List<CommonModel>();
            if (Request["wd"] == null)
            {
                Response.Redirect("index.aspx");
            }
            SearchWD = Request["wd"].ToString();

            //查询出列表
            sqlCmd.CommandText = @"select * from product                           
                                   where title like '%@title%' or content like '%@title%'
                                   order by datetime desc";

            sqlCmd.CommandText = sqlCmd.CommandText.Replace("@title", SearchWD);
            IDataReader reader = sqlCmd.ExecuteReader();
            while(reader.Read())
            {
                var models = GenerateModel(reader);
                var model = models.FirstOrDefault();
                model.content = RemoveHtml(model.content);
                if (model.content.Length >= 100)
                {
                    model.content = model.content.Substring(0, 100);
                }
                model.title = model.title.Replace(SearchWD, "<font color=\"red\">" + SearchWD + "</font>");
                model.content = model.content.Replace(SearchWD, "<font color=\"red\">" + SearchWD + "</font>");
                if (Session["user"] == null && model.categoryid == (int)category.资料下载)
                {
                    model.video = "login.aspx";
                }
                CorporateNewsList.Add(model);
            }
            reader.Close();


		}

        public string RemoveHtml(string strHtml)//取出html标签 
        {
            string strhtml = Regex.Replace(strHtml, "<[^>]*>", " ");
            return strhtml;
        } 
	}
}