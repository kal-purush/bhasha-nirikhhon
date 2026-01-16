using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using ClassLibrary;

public partial class DeleteStock : System.Web.UI.Page
{
    //var to store the primary key value of the record to be deleted
    Int32 StockNo;
    //event handler for load event
    protected void Page_Load(object sender, EventArgs e)
    {
        //get the number of the stock to be deleted from the session object
        StockNo = Convert.ToInt32(Session["StockNo"]);

    }


    protected void btnYes_Click(object sender, EventArgs e)
    {
            //create a new instance of the stock book
            clsStockCollection StockBook = new clsStockCollection();
            //find record to delete
            StockBook.ThisStock.Find(StockNo);
            //redirect back to the main page
            Response.Redirect("StockList.aspx");
        }

    
}

    