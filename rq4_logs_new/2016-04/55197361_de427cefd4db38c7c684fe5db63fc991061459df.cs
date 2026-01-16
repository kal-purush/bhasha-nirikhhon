using BusinessLayer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Restaurant
{
    public interface IBusStaff
    {
        public TableStatus tableStatus { get; set; }


        public void CheckTableMap()
        {
            //Button to open the floor status map
        }



        public void ChangeTableStatus()
        {
            if (tableStatus == TableStatus.Open)
            {
                MessageBox.Show("The table is Occupied");
                tableStatus = TableStatus.Occupied;
            }
            else if (tableStatus == TableStatus.Occupied)
            {
                MessageBox.Show("The table is Dirty");
                tableStatus = TableStatus.Dirty;
            }
            else
            {
                //DialogResult result = MessageBox.Show("Would you like to clean the table?", "caption", MessageBoxButtons.YesNo);
                //if (result == DialogResult.Yes)
                //{
                //    Console.WriteLine("Open");
                //    tableStatus = TableStatus.Open;
                //}
            }


        }
    }
}