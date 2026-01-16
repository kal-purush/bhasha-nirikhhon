using BudgetTourist.Contarcts;
using BudgetTourist.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BudgetTourist.Activities
{
    
    public class Churches : Sightseeing, IActivities
    {
        private  ChurchType churchType;

        public Churches(string name, IsPaid isPaid, double price, string location, ChurchType churchtype) : base(name, isPaid, price, location)
        {
            this.IsPaid = isPaid;
            this.ChurchType = churchtype;
        }

        public  ChurchType ChurchType { get; set; }



    }
}