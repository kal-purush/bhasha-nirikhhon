using DevExpress.Web.ASPxEditors;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

public partial class _Default : System.Web.UI.Page {
    protected void ASPxComboBox_OnItemsRequestedByFilterCondition_SQL(object source, ListEditItemsRequestedByFilterConditionEventArgs e) {
        ASPxComboBox comboBox = (ASPxComboBox)source;
        SqlDataSource1.SelectCommand =
               @"SELECT [ShipName], [ShipCity], [ShipRegion], [OrderID] FROM 
                (select [ShipName], [ShipCity], [ShipRegion], [OrderID], row_number()over(order by t.[ShipName]) as [rn] from [Orders] as t 
                where (([ShipName] + ' ' + [ShipCity] + ' ' + [ShipRegion]) LIKE @filter)) as st where st.[rn] between @startIndex and @endIndex";

        SqlDataSource1.SelectParameters.Clear();
        SqlDataSource1.SelectParameters.Add("filter", TypeCode.String, string.Format("%{0}%", e.Filter));
        SqlDataSource1.SelectParameters.Add("startIndex", TypeCode.Int64, (e.BeginIndex + 1).ToString());
        SqlDataSource1.SelectParameters.Add("endIndex", TypeCode.Int64, (e.EndIndex + 1).ToString());
        comboBox.DataSource = SqlDataSource1;
        comboBox.DataBind();
    }

    protected void ASPxComboBox_OnItemRequestedByValue_SQL(object source, ListEditItemRequestedByValueEventArgs e) {
        long value = 0;
        if (e.Value == null || !Int64.TryParse(e.Value.ToString(), out value))
            return;
        ASPxComboBox comboBox = (ASPxComboBox)source;
        SqlDataSource1.SelectCommand = @"SELECT [ShipName], [ShipCity], [ShipRegion], [OrderID] FROM Orders WHERE ([OrderID] = @OrderID) ORDER BY ShipRegion";

        SqlDataSource1.SelectParameters.Clear();
        SqlDataSource1.SelectParameters.Add("OrderID", TypeCode.Int64, e.Value.ToString());
        comboBox.DataSource = SqlDataSource1;
        comboBox.DataBind();
    }

}