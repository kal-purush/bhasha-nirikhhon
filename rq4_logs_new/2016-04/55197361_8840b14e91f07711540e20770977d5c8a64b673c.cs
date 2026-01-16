using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace WindowsFormsApplication1
{
    public class Busser : Employee
    {

        TableStatus tableStatus = TableStatus.Open;


        public void CheckTableMap()
        {
            throw new System.NotImplementedException();
        }



        public void ChangeTableStatus()
        {
            if(tableStatus == TableStatus.Open)
            {
                Console.WriteLine("Occupied");
                tableStatus = TableStatus.Occupied;
            }
            else if(tableStatus == TableStatus.Occupied)
            {
                Console.WriteLine("Dirty");
                tableStatus = TableStatus.Dirty;
            }
            else{
                DialogResult result = MessageBox.Show("Would you like to clean the table?", "caption", MessageBoxButtons.YesNo);
                if (result == DialogResult.Yes)
                {
                    Console.WriteLine("Open");
                    tableStatus = TableStatus.Open;
                }
            }

            //switch (tableStatus)
            //{
            //    case TableStatus.Open: MessageBox.Show("The table is already clean.");
            //        break;
            //    case TableStatus.Occupied: MessageBox.Show("The table is still occupied.");
            //        break;
            //    case TableStatus.Dirty: SetTableStatus();
            //        break;
            //}
             
        }

        //public void SetTableStatus(TableStatus status)
        //{

            

        //}

        enum TableStatus
        {
            Open = 0,
            Occupied = 1,
            Dirty = 2
        }
    }
}