using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

using System.ComponentModel; //Must be added for interface INotifyPropertyChanged

namespace Group6Assignment.Items
{
    /// <summary>
    /// A class which has an itemCode, an itemDesc, and a Cost
    /// </summary>
    public class clsItem : INotifyPropertyChanged
    {
        #region Attributes
        /// <summary>
        /// string type ItemCode(primary key)
        /// </summary>
        private string sItemCode;
        /// <summary>
        /// string type ItemDesc
        /// </summary>
        private string sItemDesc;
        /// <summary>
        /// decimal type Cost
        /// </summary>
        private decimal dCost;
        #endregion

        #region Properties
        /// <summary>
        /// ItemCode property
        /// Property set raises the event PropertyChanged.  This tells the DataGrid that data has changed.
        /// </summary>
        public string ItemCode
        {
            get { return sItemCode; }
            set
            {
                sItemCode = value;
                
                if (PropertyChanged != null)
                {
                    PropertyChanged(this, new PropertyChangedEventArgs("ItemCode"));
                }
            }
        }

        /// <summary>
        /// ItemDesc property
        /// Property set raises the event PropertyChanged.  This tells the DataGrid that data has changed.
        /// </summary>
        public string ItemDesc
        {
            get { return sItemDesc; }
            set
            {
                sItemDesc = value;

                if (PropertyChanged != null)
                {
                    PropertyChanged(this, new PropertyChangedEventArgs("ItemDesc"));
                }
            }
        }

        /// <summary>
        /// Cost property
        /// Property set raises the event PropertyChanged.  This tells the DataGrid that data has changed.
        /// </summary>
        public decimal Cost
        {
            get { return dCost; }
            set
            {
                dCost = value;

                if (PropertyChanged != null)
                {
                    PropertyChanged(this, new PropertyChangedEventArgs("Cost"));
                }
            }
        }
        #endregion

        /// <summary>
        /// This is the contract you make with the compiler because we are implementing the interface so
        /// we must have this event defined.  We will raise this event anytime one of our properties changes.
        /// </summary>
        public event PropertyChangedEventHandler PropertyChanged;

        #region Method
        /// <summary>
        /// Override ToString() method to print out ItemCode, ItemDesc, and Cost.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            try
            {
                return sItemCode + " " + sItemDesc + " " + dCost.ToString();
            }
            catch(Exception ex)
            {
                //Just throw the exception
                throw new Exception(MethodInfo.GetCurrentMethod().DeclaringType.Name + "." +
                                    MethodInfo.GetCurrentMethod().Name + " -> " + ex.Message);
            }
        }
        #endregion
    }
}