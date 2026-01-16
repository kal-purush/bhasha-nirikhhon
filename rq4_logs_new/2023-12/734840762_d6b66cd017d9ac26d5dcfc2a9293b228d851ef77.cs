using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DAL.Models
{
    internal class ServiceAppContext: DbContext
    {
        public DbSet<Customer> Customers { get; set; }

        public DbSet<Worker> Workers { get; set; }

        public DbSet<Service> Services { get; set; }

        public DbSet<Booking> Bookings { get; set; }

        public DbSet<Notification> Notifications { get; set; }

        public DbSet<Payment> Payments { get; set; }

        public DbSet<ReviewRating> ReviewsRatings { get; set; }


    }
}