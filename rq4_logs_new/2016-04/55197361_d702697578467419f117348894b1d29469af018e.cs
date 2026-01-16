using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HostStaff
{
    class FoodTable
    {
        //Primary Key in Database
        public int FoodTableId { get; set; }
        //Table Status (Enum)
        public TableStatus TableStatus { get; set; }

        /// <summary>
        /// Set Table Status
        /// </summary>
        /// <param name="status"></param>
        public void AssignTable(TableStatus status)
        {
            TableStatus = status;
        }

        //Get table status
        public TableStatus GetTableStatus()
        {
            if (TableStatus == TableStatus.Open)
            {
                Console.WriteLine("Table is available");
            }
            else if (TableStatus == TableStatus.Occupied)
            {
                Console.WriteLine("Table is occupied");
            }
            else if (TableStatus == TableStatus.Dirty)
            {
                Console.WriteLine("Table is dirty");
            }
            return TableStatus;
        }
    }
}