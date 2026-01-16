using EF_Project.Forms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace EF_Project
{

    public static class AppManger
    {
        /************************** Forms Decleration ****************************/
        public static Entry EntryForm;
        public static WareHouse WareHouseForm;
        public static Product ProductForm;
        public static supplier SupplierForm;
        public static Customer CustomerForm;

        /***************************** DB Entities *******************************/
        public static EFProjectEntities Entities;

        /************************** MessageBox Options ***************************/
        public static MessageBoxButtons OKButton;
        public static MessageBoxIcon ErrorIcon;
        public static MessageBoxIcon InfoIcon;

        static AppManger()
        {
            
            //DB Entities
            Entities = new EFProjectEntities();

            //MessageBox Options
            OKButton = new MessageBoxButtons();
            OKButton = MessageBoxButtons.OK;
            ErrorIcon = new MessageBoxIcon();
            ErrorIcon = MessageBoxIcon.Error;
            InfoIcon = new MessageBoxIcon();
            InfoIcon = MessageBoxIcon.Information;

            //Forms
            WareHouseForm = new WareHouse();
            ProductForm = new Product();
            SupplierForm = new supplier();
            CustomerForm = new Customer();
            EntryForm = new Entry();

        }
    }
}