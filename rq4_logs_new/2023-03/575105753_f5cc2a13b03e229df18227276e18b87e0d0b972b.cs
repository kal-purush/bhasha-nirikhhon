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
    public partial class AllPayments : System.Web.UI.Page
    {
        PaymentVoucherController PaymentVoucherController = ControllerFactory.CreatePaymentVoucherController();
        List<PaymentVoucher> paymentVoucherList = new List<PaymentVoucher>();
        protected void Page_Load(object sender, EventArgs e)
        {
            this.UnobtrusiveValidationMode = System.Web.UI.UnobtrusiveValidationMode.None;

            BindDataSource();
        }

        public void BindDataSource()
        {
            paymentVoucherList = PaymentVoucherController.GetAllPaymentVoucherWithSupplier(true);

            gvPayments.DataSource = paymentVoucherList;
            gvPayments.DataBind();
        }

        protected void btnSearch_Click(object sender, EventArgs e)
        {
            string keyWord = txtKeyWord.Text;

            paymentVoucherList = paymentVoucherList.Where(x => x.VoucherNumber.ToLower().Contains(keyWord.ToLower())
            || x.TotalAmount == Convert.ToInt32(keyWord)
            || x.PayeeName.ToLower().Contains(keyWord.ToLower())
            || x.PayeeAddress.ToLower().Contains(keyWord.ToLower())).ToList();

            gvPayments.DataSource = paymentVoucherList;
            gvPayments.DataBind();

            txtKeyWord.Text = string.Empty;
        }

        protected void btnGetAll_Click(object sender, EventArgs e)
        {
            BindDataSource();
            txtKeyWord.Text = string.Empty;
        }
    }
}