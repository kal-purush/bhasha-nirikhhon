#region using
using System;
using System.Data;
using System.Collections.Generic;

using System.Data.OleDb;
using System.Configuration;
using System.Data.SqlClient;
using System.Web.UI.WebControls;
using System.Xml;
using System.IO;
using System.Web.UI.HtmlControls;

#endregion

namespace Goldtect.ASTreeViewDemo
{
    public partial class DnDSaveDB : PageBase
    {
        protected void Page_Load( object sender, EventArgs e )
        {
            if (Session["UserName"]==null)
                Response.Redirect("Default.aspx");
            if (Request.QueryString["ID"] != null)
                lblRoot.Text = Request.QueryString["ID"];
            if (!IsPostBack)
            {
                Page.Header.DataBind();    
                BindData();
            }
        }

        private void createNewTree()
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(Server.MapPath("~/template.xml"));
            doc.Save(Server.MapPath("~/" + Session["UserName"] + ".xml"));

            AddNewNode("Sample Node");
        }

        private void BindData()
        {
            if (!File.Exists(Server.MapPath("~/" + Session["UserName"] + ".xml")))
                createNewTree();


            XmlDocument doc = new XmlDocument();
            doc.Load(Server.MapPath("~/"+Session["UserName"]+".xml"));

            ASTreeViewXMLDescriptor descripter = new ASTreeViewXMLDescriptor();

            this.astvMyTree1.DataSourceDescriptor = descripter;
            this.astvMyTree1.DataSource = doc;
            this.astvMyTree1.DataBind();

            this.astvMyTree2.DataSourceDescriptor = descripter;
            this.astvMyTree2.DataSource = doc;
            this.astvMyTree2.DataBind();

            ManageNodeTreeName();
        }

        private void ManageNodeTreeName()
        {
            ASTreeView.ASTreeNodeHandlerDelegate nodeDelegate1 = delegate(ASTreeViewNode node)
            {
                node.AdditionalAttributes.Add(new KeyValuePair<string, string>("treeName", "astvMyTree1"));
            };

            astvMyTree1.TraverseTreeNode(this.astvMyTree1.RootNode, nodeDelegate1);

            ASTreeView.ASTreeNodeHandlerDelegate nodeDelegate2 = delegate(ASTreeViewNode node)
            {
                node.AdditionalAttributes.Add(new KeyValuePair<string, string>("treeName", "astvMyTree2"));
            };

            astvMyTree2.TraverseTreeNode(this.astvMyTree2.RootNode, nodeDelegate2);
        }

        protected void btnSaveDragDrop_Click(object sender, EventArgs e)
        {
            XmlDocument doc = astvMyTree1.GetTreeViewXML();
            doc.Save(Server.MapPath("~/" + Session["UserName"] + ".xml"));
            BindData();
        }

        

        protected void astvMyTree_OnSelectedNodeChanged(object src, ASTreeViewNodeSelectedEventArgs e)
        {
            //tb1.Text = e.NodeText;
            ASTreeViewNode selectedNode = astvMyTree1.GetSelectedNode();
            if (selectedNode != null)
            {
                lblRoot.Text = selectedNode.NodeValue;

                tbItem.Text = (string)OleDbHelper.ExecuteScalar(base.NorthWindConnectionString, CommandType.Text, "select ProductName from ProductsTree where ProductID=" + lblRoot.Text);
            }
        }
        
        protected void astvMyTree2_OnSelectedNodeChanged(object src, ASTreeViewNodeSelectedEventArgs e)
        {
            //tb1.Text = e.NodeText;
            ASTreeViewNode selectedNode = astvMyTree2.GetSelectedNode();
            if (selectedNode != null)
            {
                lblRoot2.Text = selectedNode.NodeValue;
                tbItem2.Text = (string)OleDbHelper.ExecuteScalar(base.NorthWindConnectionString, CommandType.Text, "select ProductName from ProductsTree where ProductID=" + lblRoot2.Text);
            }
        }

        protected void btnUpdat_Click(object sender, EventArgs e)
        {
            if (String.IsNullOrEmpty(tbItem.Text))
                return;
            OleDbHelper.ExecuteNonQuery(base.NorthWindConnectionString, CommandType.Text, string.Format("Update ProductsTree set ProductName='{0}' where ProductId={1}", tbItem.Text, lblRoot.Text));
            String qry = "select SUBSTRING([ProductName], 1, CASE CHARINDEX(CHAR(10), [ProductName]) WHEN 0 THEN LEN([ProductName]) ELSE CHARINDEX(char(10), [ProductName]) - 1 END) as ProductName from [ProductsTree] where ProductID=" + lblRoot.Text;
            ASTreeViewNode selectedNode = astvMyTree1.FindByValue(lblRoot.Text);
            selectedNode.NodeText = (string)OleDbHelper.ExecuteScalar(base.NorthWindConnectionString, CommandType.Text, qry);

            XmlDocument doc = astvMyTree1.GetTreeViewXML();
            doc.Save(Server.MapPath("~/" + Session["UserName"] + ".xml"));
            BindData();
            
        }

        protected void btnAdd_Click(object sender, EventArgs e)
        {
            if (String.IsNullOrEmpty(tbItem.Text))
                return;
            string maxSql = string.Format("select max( productId ) from ProductsTree");
            int max = (int)OleDbHelper.ExecuteScalar(base.NorthWindConnectionString, CommandType.Text, maxSql);
            int newId = max + 1;

            OleDbHelper.ExecuteNonQuery(base.NorthWindConnectionString, CommandType.Text, string.Format("INSERT INTO ProductsTree (ProductId, ProductName, ParentId,Username) VALUES({0}, '{1}', 0,'{2}')", newId, tbItem.Text, Session["UserName"].ToString()));
            String qry = "select SUBSTRING([ProductName], 1, CASE CHARINDEX(CHAR(10), [ProductName]) WHEN 0 THEN LEN([ProductName]) ELSE CHARINDEX(char(10), [ProductName]) - 1 END) as ProductName from [ProductsTree] where ProductID=" + newId.ToString();

            ASTreeViewNode newNode = new ASTreeViewNode((string)OleDbHelper.ExecuteScalar(base.NorthWindConnectionString, CommandType.Text, qry), newId.ToString());

            ASTreeViewNode rootNode = astvMyTree1.FindByValue("root");
            rootNode.AppendChild(newNode);
            XmlDocument doc = astvMyTree1.GetTreeViewXML();
            doc.Save(Server.MapPath("~/" + Session["UserName"] + ".xml"));
            BindData();
        }

        protected void AddNewNode(string newText)
        {
            string maxSql = string.Format("select max( productId ) from ProductsTree");
            int max = (int)OleDbHelper.ExecuteScalar(base.NorthWindConnectionString, CommandType.Text, maxSql);
            int newId = max + 1;

            OleDbHelper.ExecuteNonQuery(base.NorthWindConnectionString, CommandType.Text, string.Format("INSERT INTO ProductsTree (ProductId, ProductName, ParentId,Username) VALUES({0}, '{1}', 0,'{2}')", newId, newText, Session["UserName"].ToString()));
            String qry = "select SUBSTRING([ProductName], 1, CASE CHARINDEX(CHAR(10), [ProductName]) WHEN 0 THEN LEN([ProductName]) ELSE CHARINDEX(char(10), [ProductName]) - 1 END) as ProductName from [ProductsTree] where ProductID=" + newId.ToString();
        }

        protected void btnUpdat2_Click(object sender, EventArgs e)
        {
            if (String.IsNullOrEmpty(tbItem2.Text))
                return;
            OleDbHelper.ExecuteNonQuery(base.NorthWindConnectionString, CommandType.Text, string.Format("Update ProductsTree set ProductName='{0}' where ProductId={1}", tbItem2.Text, lblRoot2.Text));
            String qry = "select SUBSTRING([ProductName], 1, CASE CHARINDEX(CHAR(10), [ProductName]) WHEN 0 THEN LEN([ProductName]) ELSE CHARINDEX(char(10), [ProductName]) - 1 END) as ProductName from [ProductsTree] where ProductID=" + lblRoot2.Text;
            ASTreeViewNode selectedNode = astvMyTree2.FindByValue(lblRoot2.Text);
            selectedNode.NodeText = (string)OleDbHelper.ExecuteScalar(base.NorthWindConnectionString, CommandType.Text, qry);
            XmlDocument doc = astvMyTree2.GetTreeViewXML();
            doc.Save(Server.MapPath("~/" + Session["UserName"] + ".xml"));
            BindData();
        }

        protected void btnAdd2_Click(object sender, EventArgs e)
        {
            if (String.IsNullOrEmpty(tbItem2.Text))
                return;

            string maxSql = string.Format("select max( productId ) from ProductsTree");
            int max = (int)OleDbHelper.ExecuteScalar(base.NorthWindConnectionString, CommandType.Text, maxSql);
            int newId = max + 1;

            OleDbHelper.ExecuteNonQuery(base.NorthWindConnectionString, CommandType.Text, string.Format("INSERT INTO ProductsTree (ProductId, ProductName, ParentId,Username) VALUES({0}, '{1}', 0,'{2}')", newId, tbItem2.Text, Session["UserName"].ToString()));
            String qry = "select SUBSTRING([ProductName], 1, CASE CHARINDEX(CHAR(10), [ProductName]) WHEN 0 THEN LEN([ProductName]) ELSE CHARINDEX(char(10), [ProductName]) - 1 END) as ProductName from [ProductsTree] where ProductID=" + newId.ToString();


            ASTreeViewNode newNode = new ASTreeViewNode((string)OleDbHelper.ExecuteScalar(base.NorthWindConnectionString, CommandType.Text, qry), newId.ToString());

            ASTreeViewNode rootNode = astvMyTree2.FindByValue("root");
            rootNode.AppendChild(newNode);
            XmlDocument doc = astvMyTree2.GetTreeViewXML();
            doc.Save(Server.MapPath("~/" + Session["UserName"] + ".xml"));
            BindData();
        }
    }
}