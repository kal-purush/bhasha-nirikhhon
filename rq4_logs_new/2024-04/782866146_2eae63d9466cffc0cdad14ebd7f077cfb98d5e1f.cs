using ConferenceRoomBooking.Models;
using Microsoft.EntityFrameworkCore;

public partial class ConferenceRoomBookingDbContext : DbContext
{
    public ConferenceRoomBookingDbContext()
    { }
    public ConferenceRoomBookingDbContext(DbContextOptions<ConferenceRoomBookingDbContext> options) : base(options)
    { }
    public virtual DbSet<Bookings> Bookings { get; set; }
    public virtual DbSet<ConferenceRooms> ConferenceRooms { get; set; }
    public virtual DbSet<ReservationHolders> ReservationHolders { get; set; }
    public virtual DbSet<UnavailabilityPeriods> UnavailabilityPeriods { get; set; }
    public virtual DbSet<Users> Users { get; set; }


}
        