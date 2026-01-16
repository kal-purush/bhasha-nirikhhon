using ManPowerCore.Common;
using ManPowerCore.Controller;
using ManPowerCore.Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace ManPowerWeb
{
    public partial class PaymentVoucherRecommendation : System.Web.UI.Page
    {

        static List<PaymentVoucher> paymentVouchersList = new List<PaymentVoucher>();
        protected void Page_Load(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {
                bindDatasource();
            }
        }

        private void bindDatasource()
        {
            PaymentVoucherController paymentVoucherController = ControllerFactory.CreatePaymentVoucherController();
            paymentVouchersList = paymentVoucherController.GetAllPaymentVoucher();

            gvPaymentVoucher.DataSource = paymentVouchersList;
            gvPaymentVoucher.DataBind();
        }

        protected void btnView_Click(object sender, EventArgs e)
        {
            int rowIndex = ((GridViewRow)((LinkButton)sender).NamingContainer).RowIndex;
            int pagesize = gvPaymentVoucher.PageSize;
            int pageindex = gvPaymentVoucher.PageIndex;
            rowIndex = (pagesize * pageindex) + rowIndex;

            PaymentVoucher paymentVoucher = new PaymentVoucher();
            paymentVoucher = paymentVouchersList[rowIndex];

            ddlSupplier.SelectedValue = paymentVoucher.SupplierId.ToString();
            txtVNumber.Text = paymentVoucher.VoucherNumber;
            txtVDate.Text = paymentVoucher.VoucherDate.ToString("yyyy-MM-dd");
            txtPName.Text = paymentVoucher.PayeeName;
            txtPAddres.Text = paymentVoucher.PayeeAddress;
            txtChequeNumber.Text = paymentVoucher.ChequeNumber;
            txtTotalAmount.Text = paymentVoucher.TotalAmount.ToString();


        }
    }
}