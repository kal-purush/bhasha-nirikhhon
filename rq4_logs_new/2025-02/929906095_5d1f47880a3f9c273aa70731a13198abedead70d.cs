using Business_Object.Model;
using GraduationAPI_EPOSHBOOKING.Model;
using Microsoft.EntityFrameworkCore;

namespace DAL
{
    public class DBContext : DbContext
    {

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {

            //optionsBuilder.UseSqlServer("Data Source=DESKTOP-B7FGES3;Initial Catalog=HomeStayBooking;Integrated Security=True;Trust Server Certificate=True");

            //server database
            optionsBuilder.UseSqlServer("Data Source=SQL1002.site4now.net;Initial Catalog=db_ab299c_homestaybooking;User ID=db_ab299c_homestaybooking_admin;Password=Admin123@;Trust Server Certificate=True");

        }
        public DbSet<Account> accounts { get; set; }
        public DbSet<Role> roles { get; set; }
        public DbSet<Profile> profiles { get; set; }
        public DbSet<Blog> blog { get; set; }
        public DbSet<BlogImage> blogImage { get; set; }
        public DbSet<CommentBlog> blogComment { get; set; }
        public DbSet<HomeStay> hotel { get; set; }
        public DbSet<HomeStayAddress> hotelAddress { get; set; }
        public DbSet<HomeStayImage> hotelImage { get; set; }
        public DbSet<HomeStayService> hotelService { get; set; }
        public DbSet<HomeStaySubService> hotelSubService { get; set; }
        public DbSet<Booking> booking { get; set; }
        public DbSet<FeedBack> feedback { get; set; }
        public DbSet<Voucher> voucher { get; set; }
        public DbSet<MyVoucher> myVoucher { get; set; }
        public DbSet<Payment> payments { get; set; }
        public DbSet<ReplyComment> replyComments { get; set; }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<MyVoucher>().HasKey(o => new { o.VoucherID, o.AccountID });
        }

    }
}