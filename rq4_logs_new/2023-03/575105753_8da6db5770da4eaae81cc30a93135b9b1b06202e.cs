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
    public partial class AllPaymentsRender : System.Web.UI.Page
    {
        public int PaymentVoucherId;
        PaymentVoucherController paymentVoucherController = ControllerFactory.CreatePaymentVoucherController();
        List<PaymentVoucher> paymentVoucherList = new List<PaymentVoucher>();
        protected void Page_Load(object sender, EventArgs e)
        {
            this.UnobtrusiveValidationMode = System.Web.UI.UnobtrusiveValidationMode.None;
            PaymentVoucherId = Convert.ToInt32(Request.QueryString["paymentVoucherID"]);

            BindData();
        }

        public void BindData()
        {
            paymentVoucherList = paymentVoucherController.GetAllPaymentVoucherWithSupplier(false);

            PaymentVoucher paymentVoucherObj = paymentVoucherList.Where(x => x.Id == PaymentVoucherId).Single();

            txtSupplier.Text = paymentVoucherObj.Supplier.Name;
            txtVNumber.Text = paymentVoucherObj.VoucherNumber;
            txtVDate.Text = paymentVoucherObj.VoucherDate.ToString("yyyy-MM-dd");
            txtPName.Text = paymentVoucherObj.PayeeName;
            txtPAddres.Text = paymentVoucherObj.PayeeAddress;
            txtChequeNumber.Text = paymentVoucherObj.ChequeNumber;
            txtTotalAmount.Text = paymentVoucherObj.TotalAmount.ToString();
            txtBankName.Text = paymentVoucherObj.BankName;
            txtBankBranch.Text = paymentVoucherObj.BankBranch;
            txtBankAcc.Text = paymentVoucherObj.BankAccount;
        }

        protected void btnBack_Click(object sender, EventArgs e)
        {
            string url = "AllPayments.aspx";
            Response.Redirect(url);
        }
    }
}