using ManPowerCore.Common;
using ManPowerCore.Controller;
using ManPowerCore.Domain;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace ManPowerWeb
{
    public partial class PaymentVoucherReport : System.Web.UI.Page
    {
        static List<PaymentVoucher> paymentVoucherReports = new List<PaymentVoucher>();

        protected void Page_Load(object sender, EventArgs e)
        {
            this.UnobtrusiveValidationMode = System.Web.UI.UnobtrusiveValidationMode.None;

            if (!IsPostBack)
            {
                BindDataSource();
            }
        }

        public void BindDataSource()
        {
            PaymentVoucherController paymentVoucherController = ControllerFactory.CreatePaymentVoucherController();
            paymentVoucherReports = paymentVoucherController.GetAllPaymentVoucherWithSupplier(true);
            gvPaymentVoucher.DataSource = paymentVoucherReports;
            gvPaymentVoucher.DataBind();
        }

        public override void VerifyRenderingInServerForm(Control control)
        {

        }
        protected void btnExportExcel_Click(object sender, EventArgs e)
        {
            BindDataSource();

            Response.Clear();
            Response.Buffer = true;
            Response.ClearContent();
            Response.ClearHeaders();
            Response.Charset = "";
            string FileName = "Payment Voucher Report - " + DateTime.Now + ".xls";
            StringWriter strwritter = new StringWriter();
            HtmlTextWriter htmltextwrtter = new HtmlTextWriter(strwritter);
            Response.Cache.SetCacheability(HttpCacheability.NoCache);
            Response.ContentType = "application/vnd.ms-excel";
            Response.AddHeader("Content-Disposition", "attachment;filename=" + FileName);
            gvPaymentVoucher.GridLines = GridLines.Both;
            gvPaymentVoucher.RenderControl(htmltextwrtter);
            Response.Write(strwritter.ToString());
            Response.End();
        }

        //protected void btnSearch_Click(object sender, EventArgs e)
        //{

        //}

        //protected void btnGetAll_Click(object sender, EventArgs e)
        //{

        //}
    }
}